plugins {
    `java-library`
    `application` // Adds the 'run' task
    id("com.gradleup.shadow") version "9.3.0"
}

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(22))
    }
}

dependencies {
    implementation("com.google.flatbuffers:flatbuffers-java:25.2.10")
    implementation("org.apache.arrow:arrow-vector:18.3.0")
    implementation("org.apache.arrow:arrow-memory-netty:18.3.0")

    // Force Jackson 2.20.1 to match Spark 4.x
    implementation(platform("com.fasterxml.jackson:jackson-bom:2.20.1"))
}

val runtimeJvmArgs = listOf(
    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
)

application {
    mainClass.set("org.gatun.server.GatunServer")
    applicationDefaultJvmArgs = runtimeJvmArgs
}

tasks.withType<JavaCompile> {
    options.release.set(22)
}

// Apply to ALL executions (run, custom JavaExec tasks, etc.)
tasks.withType<JavaExec>().configureEach {
    jvmArgs(runtimeJvmArgs)
}

// Apply to tests too (common source of “it still fails”)
tasks.withType<Test>().configureEach {
    jvmArgs(runtimeJvmArgs)
}

tasks {
    shadowJar {
        archiveBaseName.set("gatun-server")
        archiveClassifier.set("all")
        archiveVersion.set("")
        mergeServiceFiles()

        // Relocate Netty to avoid version conflicts when Spark is on classpath.
        // Arrow uses Netty for memory allocation, Spark uses a different Netty version.
        // By shading Gatun's Netty, both can coexist without conflicts.
        relocate("io.netty", "gatun.shaded.io.netty")

        // Note: Jackson is NOT shaded - we use 2.20.1 to match Spark 4.x exactly.
        // This allows Spark and Gatun to share the same Jackson classes.
    }
}
