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
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

dependencies {
    implementation("com.google.flatbuffers:flatbuffers-java:25.2.10")
    implementation("org.apache.arrow:arrow-vector:15.0.0")
    implementation("org.apache.arrow:arrow-memory-netty:15.0.0")
}

val runtimeJvmArgs = listOf(
    "--enable-preview",
    "--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
)

application {
    mainClass.set("org.gatun.server.GatunServer")
    applicationDefaultJvmArgs = runtimeJvmArgs
}

tasks.withType<JavaCompile> {
    options.compilerArgs.add("--enable-preview")
    options.release.set(21)
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
    }
}