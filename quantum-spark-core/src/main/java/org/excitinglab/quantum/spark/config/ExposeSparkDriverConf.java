package org.excitinglab.quantum.spark.config;


import org.excitinglab.quantum.config.Config;
import org.excitinglab.quantum.config.ConfigFactory;
import org.excitinglab.quantum.config.ConfigResolveOptions;
import org.excitinglab.quantum.config.ConfigValue;

import java.io.File;
import java.util.Map;


public class ExposeSparkDriverConf {


    public static void main(String[] args) throws Exception {
        Config appConfig = ConfigFactory.parseFile(new File(args[0]))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                .resolveWith(ConfigFactory.systemProperties(), ConfigResolveOptions.defaults().setAllowUnresolved(true));

        String driverPrefix = "spark.driver.";
        Config sparkConfig = appConfig.getConfig("spark");

        if (!TypesafeConfigUtils.hasSubConfig(sparkConfig, driverPrefix)) {
            System.out.println("");
        } else {
            Config sparkDriverConfig = TypesafeConfigUtils.extractSubConfig(sparkConfig, driverPrefix, true);
            StringBuilder stringBuilder = new StringBuilder();
            for (Map.Entry<String, ConfigValue> entry : sparkDriverConfig.entrySet()) {
                String key = entry.getKey();
                SparkDriverSettings settings = SparkDriverSettings.fromProperty(key);
                if (settings != null) {
                    String conf = String.format(" %s=%s ", settings.option, entry.getValue().unwrapped());
                    stringBuilder.append(conf);
                }
            }

            System.out.println(stringBuilder.toString());
        }
    }
}
