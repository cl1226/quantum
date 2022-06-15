package org.excitinglab.quantum.config.impl;

import org.excitinglab.quantum.config.ConfigException;
import org.excitinglab.quantum.config.ConfigParseOptions;
import org.excitinglab.quantum.config.ConfigRenderOptions;
import org.excitinglab.quantum.config.ConfigValue;
import org.excitinglab.quantum.config.parser.ConfigDocument;

import java.io.StringReader;
import java.util.Iterator;

final class SimpleConfigDocument implements ConfigDocument {
    private ConfigNodeRoot configNodeTree;
    private ConfigParseOptions parseOptions;

    SimpleConfigDocument(ConfigNodeRoot parsedNode, ConfigParseOptions parseOptions) {
        configNodeTree = parsedNode;
        this.parseOptions = parseOptions;
    }

    @Override
    public ConfigDocument withValueText(String path, String newValue) {
        if (newValue == null)
            throw new ConfigException.BugOrBroken("null value for " + path + " passed to withValueText");
        SimpleConfigOrigin origin = SimpleConfigOrigin.newSimple("single value parsing");
        StringReader reader = new StringReader(newValue);
        Iterator<Token> tokens = Tokenizer.tokenize(origin, reader, parseOptions.getSyntax());
        AbstractConfigNodeValue parsedValue = ConfigDocumentParser.parseValue(tokens, origin, parseOptions);
        reader.close();

        return new SimpleConfigDocument(configNodeTree.setValue(path, parsedValue, parseOptions.getSyntax()), parseOptions);
    }

    @Override
    public ConfigDocument withValue(String path, ConfigValue newValue) {
        if (newValue == null)
            throw new ConfigException.BugOrBroken("null value for " + path + " passed to withValue");
        ConfigRenderOptions options = ConfigRenderOptions.defaults();
        options = options.setOriginComments(false);
        return withValueText(path, newValue.render(options).trim());
    }

    @Override
    public ConfigDocument withoutPath(String path) {
        return new SimpleConfigDocument(configNodeTree.setValue(path, null, parseOptions.getSyntax()), parseOptions);
    }

    @Override
    public boolean hasPath(String path) {
        return configNodeTree.hasValue(path);
    }

    public String render() {
        return configNodeTree.render();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof ConfigDocument && render().equals(((ConfigDocument) other).render());
    }

    @Override
    public int hashCode() {
        return render().hashCode();
    }
}