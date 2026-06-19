plugins {
    id("me.champeau.jmh") version "0.7.2"
}

description = "FluentX Benchmarks — JMH performance benchmarks for all FluentX modules"

repositories {
    mavenCentral()
}

dependencies {
    jmh(project(":fluentx-streams"))
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
}

jmh {
    warmupIterations.set(3)
    iterations.set(5)
    fork.set(1)
    timeOnIteration.set("2s")
    warmup.set("2s")
    resultFormat.set("JSON")
    resultsFile.set(project.layout.buildDirectory.file("reports/jmh/results.json"))
    humanOutputFile.set(project.layout.buildDirectory.file("reports/jmh/results.txt"))
}
