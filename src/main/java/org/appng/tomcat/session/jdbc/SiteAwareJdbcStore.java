/*
 * Copyright 2015-2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.appng.tomcat.session.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;

import org.apache.catalina.session.JDBCStore;
import org.appng.tomcat.session.Utils;

/**
 * A {@link JDBCStore} that uses the custom {@link ObjectInputStream} provided by
 * {@link Utils#getObjectInputStream(ClassLoader, javax.servlet.ServletContext, InputStream)}
 */
public class SiteAwareJdbcStore extends JDBCStore {

	@Override
	protected ObjectInputStream getObjectInputStream(InputStream is) throws IOException {
		return Utils.getObjectInputStream(Thread.currentThread().getContextClassLoader(),
				getManager().getContext().getServletContext(), is);
	}

}
