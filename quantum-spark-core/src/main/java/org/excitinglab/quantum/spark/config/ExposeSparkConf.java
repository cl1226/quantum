package org.excitinglab.quantum.spark.config;


import org.excitinglab.quantum.config.Config;
import org.excitinglab.quantum.config.ConfigFactory;
import org.excitinglab.quantum.config.ConfigResolveOptions;
import org.excitinglab.quantum.config.ConfigValue;

import java.io.File;
import java.util.Map;

public class ExposeSparkConf {

    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, ConfigValue> entry : appConfig.getConfig("spark").entrySet()) {
            String conf = String.format(" --conf \"%s=%s\" ", entry.getKey(), entry.getValue().unwrapped());
            stringBuilder.append(conf);
        }

        System.out.print(stringBuilder.toString());
    }
}
