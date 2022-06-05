package com.xxl.conf.core.core;

import com.xxl.conf.core.XxlConfClient;
import com.xxl.conf.core.listener.XxlConfListenerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * local cache conf
 *
 * @author xuxueli 2018-02-01 19:11:25
 */
public class XxlConfLocalCacheConf {
	private static Logger logger = LoggerFactory.getLogger(XxlConfClient.class);


	// ---------------------- init/destroy ----------------------

	private static ConcurrentHashMap<String, CacheNode> localCacheRepository = null;

	private static Thread refreshThread;
	private static boolean refreshThreadStop = false;

	/**
	 * 1、将mirror的配置文件的数据读出来，将所有的key从远程再读一遍
	 * 2、将mirror的值，以及从远程读出来的值，全部放到本地缓存中（远程的会把原始内容覆盖）
	 * 3、启动一个守护线程，不停地从admin获key对应的value，如果不相等就更新本地缓存，然后将数据写入mirrorFile
	 */
	public static void init() {

		localCacheRepository = new ConcurrentHashMap<String, CacheNode>();

		// preload: mirror or remote
		Map<String, String> preConfData = new HashMap<>();

		Map<String, String> mirrorConfData = XxlConfMirrorConf.readConfMirror();

		// 1、将mirror的配置文件中的key从远程再读一遍
		Map<String, String> remoteConfData = null;
		if (mirrorConfData != null && mirrorConfData.size() > 0) {
			remoteConfData = XxlConfRemoteConf.find(mirrorConfData.keySet());
		}

		// 2、将mirror的原始内容和从远程读的内容全部放进preConfData，远程的会把原始内容覆盖
		if (mirrorConfData != null && mirrorConfData.size() > 0) {
			preConfData.putAll(mirrorConfData);
		}
		if (remoteConfData != null && remoteConfData.size() > 0) {
			preConfData.putAll(remoteConfData);
		}
		if (preConfData != null && preConfData.size() > 0) {
			for (String preKey : preConfData.keySet()) {
				set(preKey, preConfData.get(preKey), SET_TYPE.PRELOAD);
			}
		}

		// refresh thread
		// 创建一个守护线程，不停地从admin读取数据，更新
		refreshThread = new Thread(new Runnable() {
			@Override
			public void run() {
				while (!refreshThreadStop) {
					try {
						refreshCacheAndMirror();
					} catch (Exception e) {
						if (!refreshThreadStop && !(e instanceof InterruptedException)) {
							logger.error(">>>>>>>>>> xxl-conf, refresh thread error.");
							logger.error(e.getMessage(), e);
						}
					}
				}
				logger.info(">>>>>>>>>> xxl-conf, refresh thread stoped.");
			}
		});
		refreshThread.setDaemon(true);
		refreshThread.start();

		logger.info(">>>>>>>>>> xxl-conf, XxlConfLocalCacheConf init success.");
	}

	public static void destroy() {
		if (refreshThread != null) {
			refreshThreadStop = true;
			refreshThread.interrupt();
		}
	}

	/**
	 * refresh Cache And Mirror, with real-time minitor
	 * 1、守护线程不停地从admin获key对应的value，如果不相等就更新本地缓存
	 * 2、将本地缓存中的数据写入mirror
	 */
	private static void refreshCacheAndMirror() throws InterruptedException {

		if (localCacheRepository.size() == 0) {
			TimeUnit.SECONDS.sleep(3);
			return;
		}

		// monitor
		boolean monitorRet = XxlConfRemoteConf.monitor(localCacheRepository.keySet());

		// avoid fail-retry request too quick
		if (!monitorRet) {
			TimeUnit.SECONDS.sleep(10);
		}

		// refresh cache: remote > cache
		Set<String> keySet = localCacheRepository.keySet();
		if (keySet.size() > 0) {

			Map<String, String> remoteDataMap = XxlConfRemoteConf.find(keySet);
			if (remoteDataMap != null && remoteDataMap.size() > 0) {
				for (String remoteKey : remoteDataMap.keySet()) {
					String remoteData = remoteDataMap.get(remoteKey);

					CacheNode existNode = localCacheRepository.get(remoteKey);
					if (existNode != null && existNode.getValue() != null && existNode.getValue().equals(remoteData)) {
						logger.debug(">>>>>>>>>> xxl-conf: RELOAD unchange-pass [{}].", remoteKey);
					} else {
						set(remoteKey, remoteData, SET_TYPE.RELOAD);
					}

				}
			}

		}

		// refresh mirror: cache -> mirror
		Map<String, String> mirrorConfData = new HashMap<>();
		for (String key : keySet) {
			CacheNode existNode = localCacheRepository.get(key);
			mirrorConfData.put(key, existNode.getValue() != null ? existNode.getValue() : "");
		}
		XxlConfMirrorConf.writeConfMirror(mirrorConfData);

		logger.debug(">>>>>>>>>> xxl-conf, refreshCacheAndMirror success.");
	}


	// ---------------------- util ----------------------

	/**
	 * set conf (invoke listener)
	 * 1、设置到本地缓存中
	 * 2、如果是reload，执行 listener
	 */
	private static void set(String key, String value, SET_TYPE optType) {
		localCacheRepository.put(key, new CacheNode(value));
		logger.info(">>>>>>>>>> xxl-conf: {}: [{}={}]", optType, key, value);

		// value updated, invoke listener
		// 如果是reload，执行listener
		if (optType == SET_TYPE.RELOAD) {
			XxlConfListenerFactory.onChange(key, value);
		}

		// new conf, new monitor
		if (optType == SET_TYPE.SET) {
			refreshThread.interrupt();
		}
	}


	// ---------------------- inner api ----------------------

	/**
	 * get conf
	 * 从本地缓存中取
	 */
	private static CacheNode get(String key) {
		if (localCacheRepository.containsKey(key)) {
			CacheNode cacheNode = localCacheRepository.get(key);
			return cacheNode;
		}
		return null;
	}

	/**
	 * get conf
	 * 1、从本地缓存中取
	 * 2、本地缓存没有，则从远程取
	 */
	public static String get(String key, String defaultVal) {

		// level 1: 从本地缓存中取
		XxlConfLocalCacheConf.CacheNode cacheNode = XxlConfLocalCacheConf.get(key);
		if (cacheNode != null) {
			return cacheNode.getValue();
		}

		// level 2	(get-and-watch, add-local-cache)
		// 从远程取，并放入本地缓存
		String remoteData = null;
		try {
			remoteData = XxlConfRemoteConf.find(key);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}

		set(key, remoteData, SET_TYPE.SET);        // support cache null value
		if (remoteData != null) {
			return remoteData;
		}

		return defaultVal;
	}

	public enum SET_TYPE {
		SET,        // first use
		RELOAD,     // value updated
		PRELOAD     // pre hot
	}

	/**
	 * update conf  (only update exists key)  (invoke listener)
	 *
	 * @param key
	 * @param value
	 */
    /*private static void update(String key, String value) {
        if (localCacheRepository.containsKey(key)) {
            set(key, value, SET_TYPE.UPDATE );
        }
    }*/

	/**
	 * remove conf
	 *
	 * @param key
	 * @return
	 */
    /*private static void remove(String key) {
        if (localCacheRepository.containsKey(key)) {
            localCacheRepository.remove(key);
        }
        logger.info(">>>>>>>>>> xxl-conf: REMOVE: [{}]", key);
    }*/


	// ---------------------- api ----------------------

	/**
	 * local cache node
	 */
	public static class CacheNode implements Serializable {
		private static final long serialVersionUID = 42L;

		private String value;

		public CacheNode() {
		}

		public CacheNode(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		public void setValue(String value) {
			this.value = value;
		}
	}

}
