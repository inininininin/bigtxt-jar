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
import com.giveup.ValueUtils;

import oss_launcher.OssAsyncQueue;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class BigtxtLauncher {

	private JedisPool jedisPool = null;
	private DataSource dataSource = null;

	public BigtxtLauncher(JedisPool jedisPool, DataSource dataSource) {
		this.jedisPool = jedisPool;
		this.dataSource = dataSource;
	}

	public String replace(String id, String tmpData) throws Exception {
		Connection connection = null;
		Jedis jedis = null;
		try {
			connection = dataSource.getConnection();
			jedis = jedisPool.getResource();
			return replace(jedis, connection, id, tmpData);
		} catch (Exception e) {
			throw e;
		} finally {
			if (connection != null)
				connection.close();
			if (jedis != null)
				jedis.close();
		}
	}

	public static String replace(Jedis jedis, Connection connection, String id, String tmpData) throws Exception {
		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		String sql = "select data from t_bigdata where id=? ";
		String sql1 = new StringBuilder("update t_bigdata set alterTime=?,data=? where id=? ").toString();
		List sqlParams1 = null;
		boolean autoCommitSrc = false;
		try {
			autoCommitSrc = connection.getAutoCommit();
			if (autoCommitSrc)
				connection.setAutoCommit(false);

			if (id == null)
				return insert(connection, tmpData);

			pst = connection.prepareStatement(sql);
			Map oldRow = JdbcUtils.parseResultSetOfOne(JdbcUtils.runQuery(pst, sql, id));
			pst.close();

			if (oldRow == null)
				return insert(connection, tmpData);

			OssAsyncQueue.realizeSync(connection, HtmlUtils.extractUrls(tmpData));

			String data = OssAsyncQueue.tmpToRealUrls(tmpData);
			OssAsyncQueue.deleteSync(connection, 0,
					HtmlUtils.extractOffUrls(ValueUtils.toString(oldRow.get("data")), data));

			sqlParams1 = new ArrayList();
			sqlParams1.add(new Date());
			sqlParams1.add(data);
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
			if (connection != null && autoCommitSrc)
				connection.commit();
			throw e;
		} finally {
			if (connection != null && autoCommitSrc)
				connection.setAutoCommit(autoCommitSrc);
			if (pst != null)
				pst.close();
			if (pst1 != null)
				pst1.close();
		}
	}

	public String insert(String tmpData) throws Exception {
		Connection connection = null;
		try {
			connection = dataSource.getConnection();
			return insert(tmpData);
		} catch (Exception e) {
			throw e;
		} finally {
			if (connection != null)
				connection.close();
		}
	}

	public static String insert(Connection connection, String tmpData) throws Exception {
		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		String sql = "select data from t_bigdata where id=? ";
		String sql1 = "insert into t_bigdata (id,data,alterTime,addTime) values(?,?,?,?)";
		List sqlParams1 = null;
		boolean autoCommitSrc = false;
		try {
			autoCommitSrc = connection.getAutoCommit();
			if (autoCommitSrc)
				connection.setAutoCommit(false);

			String id = 1 + RandomStringUtils.randomNumeric(11);
			sqlParams1 = new ArrayList();
			sqlParams1.add(id);
			sqlParams1.add(OssAsyncQueue.tmpToRealUrls(tmpData));
			sqlParams1.add(new Date());
			sqlParams1.add(new Date());
			pst1 = connection.prepareStatement(sql1);
			JdbcUtils.runUpdate(pst1, sql1, sqlParams1);
			pst1.close();

			OssAsyncQueue.realizeSync(connection, HtmlUtils.extractUrls(tmpData));

			if (autoCommitSrc)
				connection.commit();
			return id;
		} catch (Exception e) {
			if (connection != null && autoCommitSrc)
				connection.commit();
			throw e;
		} finally {
			if (connection != null && autoCommitSrc)
				connection.setAutoCommit(autoCommitSrc);
			if (pst != null)
				pst.close();
			if (pst1 != null)
				pst1.close();
		}
	}

	public String delete(String id) throws Exception {
		Connection connection = null;
		Jedis jedis = null;
		try {
			connection = dataSource.getConnection();
			jedis = jedisPool.getResource();
			return delete(jedis, connection, id);
		} catch (Exception e) {
			throw e;
		} finally {
			if (connection != null)
				connection.close();
			if (jedis != null)
				jedis.close();
		}
	}

	public static String delete(Jedis jedis, Connection connection, String id) throws Exception {
		PreparedStatement pst = null;
		PreparedStatement pst1 = null;
		String sql = "select data from t_bigdata where id=? ";
		String sql1 = "delete from t_bigdata where id=? ";
		boolean autoCommitSrc = false;
		try {
			autoCommitSrc = connection.getAutoCommit();
			if (autoCommitSrc)
				connection.setAutoCommit(false);
			pst = connection.prepareStatement(sql);
			Map row = JdbcUtils.parseResultSetOfOne(JdbcUtils.runQuery(pst, sql, id));
			pst.close();

			if (row == null)
				return id;

			pst1 = connection.prepareStatement(sql1);
			JdbcUtils.runUpdate(pst1, sql1, id);
			pst1.close();

			OssAsyncQueue.deleteSync(connection, 0, HtmlUtils.extractUrls(ValueUtils.toString(row.get("data"))));

			if (autoCommitSrc)
				connection.commit();

			String bigdataRedisKey = "bigdata" + id;
			jedis.del(bigdataRedisKey);

			return id;
		} catch (Exception e) {
			if (connection != null && autoCommitSrc)
				connection.commit();
			throw e;
		} finally {
			if (connection != null && autoCommitSrc)
				connection.setAutoCommit(autoCommitSrc);
			if (pst != null)
				pst.close();
			if (pst1 != null)
				pst1.close();
		}
	}

	public String getData(String id) throws Exception {
		Connection connection = null;
		Jedis jedis = null;
		try {
			connection = dataSource.getConnection();
			jedis = jedisPool.getResource();
			return getData(jedis, connection, id);
		} catch (Exception e) {
			throw e;
		} finally {
			if (connection != null)
				connection.close();
			if (jedis != null)
				jedis.close();
		}
	}

	public static String getData(Jedis jedis, Connection connection, String id) throws Exception {
		PreparedStatement pst = null;
		String sql = null;
		try {
			// 获取请求参数
			String bigdataRedisKey = "bigdata" + id;
			String data = jedis.get(bigdataRedisKey);
			if (data == null || data.isEmpty()) {
				sql = "select data from t_bigdata where id=?";
				pst = connection.prepareStatement(sql);
				Map row = JdbcUtils.parseResultSetOfOne(JdbcUtils.runQuery(pst, sql, id));
				pst.close();
				if (row != null)
					data = (String) row.get("data");

				if (data != null && !data.isEmpty())
					jedis.setex(bigdataRedisKey, 1 * 24 * 60, data);
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

	public String getFirstVideo(String id) throws Exception {
		String data = getData(id);
		List<String> list = new ArrayList<String>();
		Pattern pa = Pattern.compile("<video.*?src=('|\")(.*?)('|\")>");
		Matcher ma = pa.matcher(data);
		while (ma.find())// 寻找符合el的字串
		{
			return ma.group(2);
		}
		return null;
	}
}
