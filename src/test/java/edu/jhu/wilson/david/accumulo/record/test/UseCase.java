package edu.jhu.wilson.david.accumulo.record.test;

import java.io.File;
import java.util.Set;

public class UseCase {

	private int expectedResults;
	private File file;
	private String name;
	private Set<String> authorizationStrings;

	public final int getExpectedResults() {
		return expectedResults;
	}

	public final void setExpectedResults(int expectedResults) {
		this.expectedResults = expectedResults;
	}

	public final File getFile() {
		return file;
	}

	public final void setFile(File file) {
		this.file = file;
	}

	public final String getName() {
		return name;
	}

	public final void setName(String name) {
		this.name = name;
	}

	public Set<String> getAuthorizationStrings() {
		return authorizationStrings;
	}

	public void setAuthorizationStrings(Set<String> authorizationStrings) {
		this.authorizationStrings = authorizationStrings;
	}
}