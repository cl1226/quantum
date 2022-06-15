/**
 *   Copyright (C) 2011-2012 Typesafe Inc. <http://typesafe.com>
 */
package org.excitinglab.quantum.config.impl;

import org.excitinglab.quantum.config.ConfigIncluderClasspath;
import org.excitinglab.quantum.config.ConfigIncluderFile;
import org.excitinglab.quantum.config.ConfigIncluderURL;
import org.excitinglab.quantum.config.ConfigIncluder;

interface FullIncluder extends ConfigIncluder, ConfigIncluderFile, ConfigIncluderURL,
        ConfigIncluderClasspath {

}
