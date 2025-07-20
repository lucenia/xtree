# Contributing to Lucenia XTree

Thank you for your interest in contributing to Lucenia XTree! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

To build Lucenia XTree, ensure the following are installed:

- **C++17 compatible compiler** (e.g., `g++` ≥ 7, `clang++` ≥ 5)
- **CMake** ≥ 3.14
- **Boost libraries**:
  - `libboost-system`
  - `libboost-filesystem`
  - `libboost-thread`
  - `libboost-iostreams`
- **JDK** ≥ 21 (for JNI headers)
- **Google Test** (automatically downloaded during build)

#### Platform-specific Installation

**Ubuntu/Debian:**
```bash
sudo apt install build-essential cmake libboost-all-dev default-jdk
```

**macOS (Homebrew):**
```bash
brew install cmake boost openjdk@21
```

**Windows:**
- Install Visual Studio with C++ development tools
- Install CMake from https://cmake.org
- Install Boost from https://www.boost.org

### Building the Project

#### Quick Start

```bash
# Clone the repository
git clone https://github.com/lucenia/xtree.git
cd xtree

# Build the project
./gradlew build
```

#### Build Commands

- **Build without tests:** `./gradlew assemble`
- **Build and run tests:** `./gradlew build`
- **Run tests only:** `./gradlew test`
- **Clean build artifacts:** `./gradlew clean`

### Build Modes

#### 1. Development Build (Default)

For regular development, the build includes a Git SHA:

```bash
./gradlew build
# Output: libXTree-<platform>-0.5.0-g17eb7b4.so
```

#### 2. Snapshot Build

For pre-release testing and CI:

```bash
./gradlew build -Psnapshot
# Output: libXTree-<platform>-0.5.0-SNAPSHOT.so
```

#### 3. Release Build

For official releases:

```bash
# Ensure clean working directory
git status  # Should show no changes

# Tag the release
git tag v0.5.0
git push --tags

# Build
./gradlew build
# Output: libXTree-<platform>-0.5.0.so
```

### Versioning

The project version is defined in `buildSrc/version.properties`:

```properties
lucenia = 0.5.0
```

This version is used by both Gradle and CMake build systems.

## Development Guidelines

### Code Style

- Follow modern C++17 best practices
- Use RAII and smart pointers for memory management
- Prefer `const` correctness
- Use meaningful variable and function names
- Keep functions focused and small

### Performance Considerations

XTree is designed for high-performance spatial indexing. When contributing:

- Minimize allocations in hot paths
- Use cache-friendly data structures
- Profile before and after significant changes
- Consider using the provided performance macros in `perf_macros.h`

### Memory Safety

- Always check for NULL pointers before dereferencing
- Use bounds checking for array access
- Prefer stack allocation over heap when possible
- Ensure proper cleanup in destructors

## Testing

See [TESTING.md](TESTING.md) for detailed testing guidelines.

Quick test commands:
```bash
# Run all tests
./gradlew test

# Run native tests directly
./build/native/bin/xtree_tests

# Run specific test suite
./build/native/bin/xtree_tests --gtest_filter=KeyMBRTest*
```

## Submitting Changes

1. **Fork the repository** on GitHub
2. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following the guidelines above
4. **Add tests** for new functionality
5. **Ensure all tests pass**:
   ```bash
   ./gradlew clean build
   ```
6. **Commit with a descriptive message**:
   ```bash
   git commit -m "Add feature: brief description of changes"
   ```
7. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```
8. **Create a Pull Request** on GitHub

### Pull Request Guidelines

- Provide a clear description of the changes
- Reference any related issues
- Include test results and performance impact if applicable
- Ensure CI passes before requesting review

## Project Structure

```
xtree/
├── core/
│   └── src/
│       └── main/
│           └── cpp/
│               ├── CMakeLists.txt    # CMake configuration
│               ├── src/              # Source files
│               │   ├── xtree.h       # Main XTree header
│               │   ├── keymbr.h/cpp  # Spatial MBR implementation
│               │   └── ...
│               └── test/             # Test files
├── buildSrc/
│   └── version.properties            # Version configuration
├── build.gradle                      # Root Gradle build
└── settings.gradle                   # Gradle settings
```

## Maintaining Build Tools

### Upgrading Gradle

The project uses Gradle with automatic SHA-256 verification, similar to OpenSearch's approach.

#### Upgrading the Wrapper

To upgrade Gradle, simply run:

```bash
./gradlew wrapper --gradle-version 8.15.0
```

This will:
1. Download and update the Gradle wrapper
2. **Automatically fetch and add the SHA-256 checksum**
3. Update all wrapper files

The SHA-256 checksum is automatically added to `gradle-wrapper.properties` for security.

#### Verifying Wrapper Integrity

To verify the Gradle wrapper's SHA-256 checksum:

```bash
./gradlew verifyWrapper
```

This task will:
- Check that a SHA-256 checksum exists
- Fetch the official checksum from Gradle
- Verify they match

## Getting Help

- Open an issue on GitHub for bugs or feature requests
- Join the Lucenia community discussions
- Check existing issues before creating new ones

## License

By contributing to Lucenia XTree, you agree that your contributions will be licensed under the Server Side Public License, version 1 (SSPL-1.0).