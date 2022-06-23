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
package org.appng.tomcat.session;

public class Constants {

	/**
	 * the fully qualified classname of the {@link java.io.ObjectInputStream} implementation to be used when restoring a
	 * {@link org.apache.catalina.Session} from a byte sequence.
	 */
	public static final String INPUT_STREAM_CLASS = "org.appng.api.support.SiteAwareObjectInputStream";
}
