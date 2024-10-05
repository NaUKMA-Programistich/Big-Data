plugins {
    kotlin("jvm") version "2.0.0"
    application
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "com.programmistich.MatrixMapReduce"
version = "1.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib:2.0.0")

    implementation("org.apache.hadoop:hadoop-common:3.4.0")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.4.0")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-common:3.4.0")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-jobclient:3.4.0")

    implementation("org.slf4j:slf4j-api:2.0.16")
    implementation("org.slf4j:slf4j-log4j12:2.0.16")
}

kotlin {
    jvmToolchain(8)
}

application {
    mainClass = "MatrixMapReduce"
}

tasks {
    shadowJar {
        archiveClassifier.set("")
        mergeServiceFiles()
    }

    build {
        dependsOn(shadowJar)
    }
}