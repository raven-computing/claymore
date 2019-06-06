# Claymore: Additional Core Libraries for Java

[![Release](https://img.shields.io/badge/release-2.5.0-blue.svg)](https://raven-computing.com/products/claymore) [![Download](https://img.shields.io/badge/download-jar-blue.svg)](https://raven-computing.com/products/claymore/releases/claymore-2.5.0.zip)

Claymore is a set of core libraries which are often used in our Java projects. We wanted to make some of them open source in the hope that others might find them useful. The provided APIs are complementary to the native Java libraries, for example by adding support for DataFrames, readers/writers for CSV-files, a parser for command line arguments, and more.

## Getting Started

Claymore is available on the Maven central repository. In order to use Claymore in your project just add the appropriate dependency information to your build system. Of course you can also download and add all JARs manually. Please see the documentation of your IDE on how to add external JARs to your build path. Make sure to include sources and javadocs when adding Claymore manually to your build.

#### Maven
```
<dependency>
  <groupId>com.raven-computing</groupId>
  <artifactId>claymore</artifactId>
  <version>2.5.0</version>
</dependency>
```

#### Gradle
```
implementation 'com.raven-computing:claymore:2.5.0'
```

For more information see [search.maven.org](https://search.maven.org/artifact/com.raven-computing/claymore/2.5.0/jar).

## Compatibility

Claymore requires **Java 8** or higher. 

## Documentation

Take a look at the [Developer Documentation](https://github.com/raven-computing/claymore/wiki/Home).

## License

The Claymore library is licensed under the Apache License Version 2 - see the [LICENSE](LICENSE) for details.


