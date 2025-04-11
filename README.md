# Lucenia XTree

Lucenia XTree is a native C++ dimensional index library used as part of the Lucenia stack. It supports advanced numeric, spatial, and vector indexing and search performance with versioned shared library builds. It provides consistent and predictive logarithmic performance over BKD with memory safety guarantees; far outperforming current lucene numeric indexing.

---

## Build Modes

The build system supports **three types of builds**: dirty builds (default), snapshot builds, and release builds.

### 1. Dirty Build (Default)

When you build without any flags and your Git working directory is not clean or not tagged, the version will include a short Git SHA:

```bash
./gradlew build
```

**Output Example:**

```
libXTree-Linux-amd64-64-0.5.0-g17eb7b4.so
```

This is the default for ongoing development.

---

### 2. Snapshot Build

To build a pre-release snapshot version, pass the `-PversionSuffixOverride` property:

```bash
./gradlew build -PversionSuffixOverride=-SNAPSHOT
```

**Output Example:**

```
libXTree-Linux-amd64-64-0.5.0-SNAPSHOT.so
```

Use this for CI and integration testing prior to release.

---

### 3. Release Build

To produce a clean release artifact (no Git SHA, no SNAPSHOT suffix):

1. Make sure your Git repo is **clean**.
2. Tag the current commit with a version tag:

```bash
git tag v0.5.0
git push --tags
```

3. Then run:

```bash
./gradlew build
```

**Output Example:**

```
libXTree-Linux-amd64-64-0.5.0.so
```

---

## Clean Build Artifacts

To clean native output directories:

```bash
./gradlew clean
```

This removes the `build/native/` directory.

---

## Versioning Source

The canonical version of XTree is defined in:

```
buildSrc/version.properties
```

This file must include:

```properties
lucenia = 0.5.0
```

That value drives all version resolution (alongside Git).


**Development Requirements**

To build the Lucenia XTree native extension, ensure the following are installed:

- C++17 compatible compiler (e.g., `g++` ≥ 7)
- CMake ≥ 3.14
- Boost libraries:
  - `libboost-system`
  - `libboost-filesystem`
  - `libboost-thread`
  - `libboost-iostreams`
- JNI headers (typically provided by the JDK)

For Ubuntu/Debian-based systems:

```bash
sudo apt install build-essential cmake libboost-all-dev default-jdk


---

For more information on Lucenia, visit [https://lucenia.io](https://lucenia.io).


