# Claymore: Additional Core Libraries for Java

[![Release](https://img.shields.io/badge/Release-4.0.2-blue.svg)](https://raven-computing.com/products/claymore) [![Download](https://img.shields.io/badge/Download-jar-blue.svg)](https://repo1.maven.org/maven2/com/raven-computing/claymore/4.0.2/)

Claymore is a set of core libraries which are often used in our Java projects. We wanted to make some of them open source in the hope that others might find them useful. The provided APIs are complementary to the standard Java libraries, for example by adding support for DataFrames, readers/writers for CSV-files, a parser for command line arguments, and more.

## Getting Started

Claymore is available on the Maven central repository. In order to use Claymore in your project just add the appropriate dependency information to your build system. Of course you can also download and add all JARs manually. Please see the documentation of your IDE on how to add external JARs to your build path. Make sure to include sources and javadocs when adding Claymore manually to your build.

#### Maven
```
<dependency>
  <groupId>com.raven-computing</groupId>
  <artifactId>claymore</artifactId>
  <version>4.0.2</version>
</dependency>
```

#### Gradle
```
implementation 'com.raven-computing:claymore:4.0.2'
```

For more information see [search.maven.org](https://search.maven.org/artifact/com.raven-computing/claymore/4.0.2/jar).

## Compatibility

Claymore requires **Java 8** or higher.

## Documentation

Take a look at the [Developer Documentation](https://github.com/raven-computing/claymore/wiki/Home).

For further information on the APIs, please see the provided [Javadocs](https://www.javadoc.io/doc/com.raven-computing/claymore/latest/index.html).

## Development

If you want to change code of this library or build Claymore from source, you can do so by cloning this repository. We use Maven as our build system.

### Build

Use Maven to build the artifacts:
```
mvn package
```

### Running Tests

Execute all unit test:
```
mvn test
```

## License

The Claymore library is licensed under the Apache License Version 2 - see the [LICENSE](LICENSE) for details.

