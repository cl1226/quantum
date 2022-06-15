package org.excitinglab.quantum.config.impl;

import org.excitinglab.quantum.config.ConfigMergeable;
import org.excitinglab.quantum.config.ConfigValue;

interface MergeableValue extends ConfigMergeable {
    // converts a Config to its root object and a ConfigValue to itself
    ConfigValue toFallbackValue();
}
