package com.greatbee.core.util;

import com.greatbee.base.bean.DBException;
import org.springframework.data.redis.core.RedisTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * RedisBuildUtil
 *
 * @author xiaobc
 * @date 17/9/25
 */
public class RedisBuildUtil {

    public static final String REDIS_URL="cache.redis";
    public static final String REDIS_DATABASE="cache.redis.database";
    public static final String REDIS_POOL_MAX_TOTAL="cache.redis.pool.maxTotal";
    public static final String REDIS_POOL_MAX_IDLE="cache.redis.pool.maxIdle";
    public static final String REDIS_POOL_MIN_IDLE="cache.redis.pool.minIdle";
    public static final String REDIS_DEFAULT_EXPIRATION="cache.redis.default.expiration";

    public static final String REDIS_CONFIG_PATH="redis/";

    public static final String RedisConfig = "RedisConfig";
    public static final String RedisConnFactory = "RedisConnFactory";
    public static final String RedisTemplate = "Template";

    /**
     * 获取某个目录下的所有文件列表
     * @param dirPath
     * @return
     */
    public static File[] getRedisConfigFiles(String dirPath){
        File file=new File(dirPath);
        return file.listFiles();
    }

    public static File[] getRedisConfigFiles(){
        //获取文件系统中的classPath的路径
        String classPath= Thread.currentThread().getContextClassLoader().getResource("").getPath();
        return getRedisConfigFiles(classPath + File.separator + REDIS_CONFIG_PATH);
    }

    /**
     * 读取redis的配置文件，获取里面的配置项
     * @param file
     * @return
     */
    public static Properties filterRedis(String file) {
        URL url = Thread.currentThread().getContextClassLoader().getResource(file);
        Properties prop = new Properties();
        try {
            URLConnection conn = url.openConnection();
            InputStream in = conn.getInputStream();
            BufferedReader bf = new BufferedReader(new InputStreamReader(in, "utf-8"));
            prop.load(bf);
            return prop;
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    /**
     * 根据redis别名获取redis配置属性
     * @param alias
     * @return
     */
    public static Map<String,String> getRedisConfig(String alias){
        Properties pops = filterRedis("redis/" + alias + ".properties");
        Map<String,String> map = new HashMap<>();
        if(pops.containsKey(REDIS_URL)){
            map.put(REDIS_URL, pops.getProperty(REDIS_URL));
        }
        if(pops.containsKey(REDIS_POOL_MAX_TOTAL)){
            map.put(REDIS_POOL_MAX_TOTAL,pops.getProperty(REDIS_POOL_MAX_TOTAL));
        }
        if(pops.containsKey(REDIS_POOL_MAX_IDLE)){
            map.put(REDIS_POOL_MAX_IDLE,pops.getProperty(REDIS_POOL_MAX_IDLE));
        }
        if(pops.containsKey(REDIS_POOL_MIN_IDLE)){
            map.put(REDIS_POOL_MIN_IDLE,pops.getProperty(REDIS_POOL_MIN_IDLE));
        }
        return map;
    }

    /**
     * 根据redis配置别名获取redisTemplate
     * @param alias
     * @return
     * @throws DBException
     */
    public static RedisTemplate getRedisTemplate(String alias) throws DBException{
        return ((RedisTemplate)((Map) SpringContextUtil.getBean("redisMap")).get(alias));
    }

//    public static void main(String[] args) {
//        File[] files = getRedisConfigFiles();
//        for(int i=0;i<files.length;i++){
//            File f = files[i];
//            System.out.println(f.getPath());
//            Properties pop = filterRedis(REDIS_CONFIG_PATH+f.getName());
//            System.out.println(pop.getProperty(REDIS_URL));
//        }
//    }

}
