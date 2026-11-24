package com.example.outbox.loadtest;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/** Um profile = um arquivo .properties; o nome do arquivo (sem extensão) é o nome do profile. */
record LoadProfile(String name, Map<String, String> props) {

	static LoadProfile load(Path file) throws IOException {
		Properties p = new Properties();
		try (Reader r = Files.newBufferedReader(file)) {
			p.load(r);
		}
		Map<String, String> map = new TreeMap<>();
		p.forEach((k, v) -> map.put(k.toString(), v.toString().trim()));
		String fileName = file.getFileName().toString();
		return new LoadProfile(fileName.substring(0, fileName.lastIndexOf('.')), map);
	}

	String broker() {
		return str("broker", "inmemory");
	}

	String str(String key, String def) {
		return props.getOrDefault(key, def);
	}

	int num(String key, int def) {
		return Integer.parseInt(props.getOrDefault(key, String.valueOf(def)));
	}

	boolean flag(String key, boolean def) {
		return Boolean.parseBoolean(props.getOrDefault(key, String.valueOf(def)));
	}
}
