package com.zhongan.bigtxt.launcher;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.apache.commons.lang3.RandomStringUtils;

import com.giveup.HtmlUtils;
import com.giveup.JdbcUtils;
import com.giveup.OtherUtils;
import com.giveup.ValueUtils;

import oss.launcher.OssLauncher;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class BigtxtLauncher {

	private JedisPool jedisPool = null;
	private DataSource dataSource = null;
	private OssLauncher ossLauncher = null;

	public BigtxtLauncher(JedisPool jedisPool, DataSource dataSource, OssLauncher ossLauncher) {
		this.jedisPool = jedisPool;
		this.dataSource = dataSource;
		this.ossLauncher = ossLauncher;
	}

	public String replace(String id, String data, Jedis jedis, Connection connection) throws Exception {
		if (data == null)
			return id;

		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		String sql1 = new StringBuilder("update t_bigtxt set alterTime=?,data=?,innerUrls=? where id=? ").toString();
		List sqlParams1 = null;
		boolean autoCommitSrc = false;
		try {
			autoCommitSrc = connection.getAutoCommit();
			if (autoCommitSrc)
				connection.setAutoCommit(false);
			if (id == null)
				return insert(data, connection);

			if (data.isEmpty()) {
				delete(id, jedis, connection);
				return "";
			}

			Map oldRow = JdbcUtils.runQueryOne(connection, "select data from t_bigtxt where id=? ", id);

			if (oldRow == null)
				return insert(data, connection);

			List<String> newInnerUrls = HtmlUtils.extractUrls(data);
			ossLauncher.realize(connection, newInnerUrls);

			List<String> oldInnerUrls = HtmlUtils.extractUrls(ValueUtils.toString(oldRow.get("data")));
			ossLauncher.delete(connection, OtherUtils.extractOffStrs(oldInnerUrls, newInnerUrls, true));

			sqlParams1 = new ArrayList();
			sqlParams1.add(new Date());
			sqlParams1.add(data);
			sqlParams1.add(newInnerUrls.toString().replaceAll("\\[|\\]", ""));
			sqlParams1.add(id);
			pst1 = connection.prepareStatement(sql1);
			JdbcUtils.runUpdate(pst1, sql1, sqlParams1);
			pst1.close();

			if (autoCommitSrc)
				connection.commit();

			String bigdataRedisKey = "bigdata" + id;
			jedis.del(bigdataRedisKey);

			return id;
		} catch (Exception e) {
			if (autoCommitSrc)
				connection.rollback();
			throw e;
		} finally {
			if (autoCommitSrc)
				connection.setAutoCommit(autoCommitSrc);
			if (pst != null)
				pst.close();
			if (pst1 != null)
				pst1.close();
		}
	}

	public String insert(String data, Connection connection) throws Exception {
		if (data == null || data.isEmpty())
			return null;
		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		String sql = "select data from t_bigtxt where id=? ";
		String sql1 = "insert into t_bigtxt (id,data,innerUrls,alterTime,addTime) values(?,?,?,?,?)";
		List sqlParams1 = null;
		boolean autoCommitSrc = false;
		try {
			autoCommitSrc = connection.getAutoCommit();
			if (autoCommitSrc)
				connection.setAutoCommit(false);

			List<String> innerUrls = HtmlUtils.extractUrls(data);

			String id = 1 + RandomStringUtils.randomNumeric(11);
			sqlParams1 = new ArrayList();
			sqlParams1.add(id);
			sqlParams1.add(data);
			sqlParams1.add(innerUrls.toString().replaceAll("\\[|\\]", ""));
			sqlParams1.add(new Date());
			sqlParams1.add(new Date());
			pst1 = connection.prepareStatement(sql1);
			JdbcUtils.runUpdate(pst1, sql1, sqlParams1);
			pst1.close();

			ossLauncher.realize(connection, innerUrls);

			if (autoCommitSrc)
				connection.commit();
			return id;
		} catch (Exception e) {
			if (autoCommitSrc)
				connection.rollback();
			throw e;
		} finally {
			if (autoCommitSrc)
				connection.setAutoCommit(autoCommitSrc);
			if (pst != null)
				pst.close();
			if (pst1 != null)
				pst1.close();
		}
	}

	public String delete(String id, Jedis jedis, Connection connection) throws Exception {
		boolean autoCommitSrc = false;
		try {
			autoCommitSrc = connection.getAutoCommit();
			if (autoCommitSrc)
				connection.setAutoCommit(false);
			Map row = JdbcUtils.runQueryOne(connection, "select data from t_bigtxt where id=? ", id);

			if (row == null)
				return id;

			JdbcUtils.runUpdate(connection, "delete from t_bigtxt where id=? ", id);

			ossLauncher.delete(connection, HtmlUtils.extractUrls(ValueUtils.toString(row.get("data"))));

			if (autoCommitSrc)
				connection.commit();

			jedis.del("bigdata" + id);

			return id;
		} catch (Exception e) {
			if (autoCommitSrc)
				connection.rollback();
			throw e;
		} finally {
			if (autoCommitSrc)
				connection.setAutoCommit(autoCommitSrc);
		}
	}

	public static String getData(String id, Jedis jedis, Connection connection) throws Exception {
		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		String sql = null;
		String sql1 = new StringBuilder("update t_bigtxt set innerUrls=? where id=? ").toString();
		List sqlParams1 = null;
		try {
			// 获取请求参数
			String data = jedis.get("bigdata" + id);
			if (data == null || data.isEmpty()) {
				data = JdbcUtils.runQueryOneString(connection, "select data from t_bigtxt where id=?", id);
				if (data != null && !data.isEmpty())
					jedis.setex("bigdata" + id, 1 * 24 * 60, data);
			}
			return data;
		} catch (Exception e) {
			throw e;
		} finally {
			// 释放资源
			if (pst != null)
				pst.close();
		}
	}

	// public String getFirstMedia(String id) throws Exception {
	// String data = getData(id);
	// List<String> list = new ArrayList<String>();
	// Pattern pa = Pattern.compile("<video.*?src=('|\")(.*?)('|\")>");
	// Matcher ma = pa.matcher(data);
	// while (ma.find())// 寻找符合el的字串
	// {
	// return ma.group(2);
	// }
	// return null;
	// }
}
