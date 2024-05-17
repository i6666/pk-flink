package com.pk.flink.basic.config;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.HashMap;

public class ConfigInit {

    private final static Logger log = LoggerFactory.getLogger(ConfigInit.class);

    public static void initLoadConfig() {
        InputStream application = ConfigInit.class.getClassLoader().getResourceAsStream("application-dev.yml");
        HashMap<String, Object> load = new Yaml().load(application);
        JSONObject config = new JSONObject(load);
        loadSocketConfig(config.getJSONObject("socket"));

        log.info("config init success {}", config);

    }

    private static void loadSocketConfig(JSONObject socket) {
        SocketConfig.host = socket.getString("host");
        SocketConfig.port = socket.getInteger("port");
    }
}
