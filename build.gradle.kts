import java.io.ByteArrayOutputStream
import java.util.Properties

buildscript {
    repositories {
        jcenter()
    }
    dependencies {
        classpath("com.opencsv:opencsv:4.5")
    }
}

plugins {
    java
    jacoco
    maven
    `maven-publish`
    id("com.diffplug.gradle.spotless") version "3.21.1"
}

repositories {
    jcenter()
}

dependencies {
    implementation("org.json:json:20180130")
    implementation("org.apache.httpcomponents:httpclient:4.5.10")
    implementation("org.slf4j:slf4j-api:1.7.29")

    testImplementation("org.testng:testng:6.14.2")
    testImplementation("org.mockito:mockito-core:2.18.3")
    testImplementation("ch.qos.logback:logback-classic:1.2.3")
    testImplementation("commons-io:commons-io:2.6")
}

project.group = "ru.rambler.usermodel"

spotless {
    java {
        importOrderFile("spotless.importorder")
        eclipse().configFile("spotless.eclipseformat.xml")
        custom("Lambda fix", {
            it.replace("} )", "})")
                .replace("} ,", "},")
        })
        custom("Long literal fix", {
            it.replace(Regex("([0-9_]+) [Ll]"), "$1L")
        })
    }
}

fun make_version(): String {
    val stdout = ByteArrayOutputStream()
    exec {
        executable("bash")
        args(
                "-c",
                "git describe --always --tags | sed -r \'s/^(.*)-(.*)-(.*)/\\1.\\2-\\3/\'"
        )
        standardOutput = stdout
    }
    return stdout.toString().trim()
}

project.version = make_version()

tasks.named<Task>("build"){
    dependsOn("spotlessCheck")
}

tasks.test {
    onlyIf { project.hasProperty("integration.test.props") }
    useTestNG()
    options {
        systemProperties(
                Properties().run {
                    load(file(project.properties.getOrDefault("integration.test.props", "test.properties") as String).inputStream())
                    toMap()
                } as Map<String, Any>
        )
    }
}

jacoco {
    toolVersion = "0.8.1"
}

tasks.jacocoTestReport {
    reports {
        xml.isEnabled = false
        csv.isEnabled = true
        html.isEnabled = true
        html.destination = file("$buildDir/jacocoHtml")
    }
}

publishing {
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/rambler-digital-solutions/java-webhdfs")
            credentials {
                username = project.findProperty("gpr.user") as String? ?: System.getenv("USERNAME")
                password = project.findProperty("gpr.key") as String? ?: System.getenv("PASSWORD")
            }
        }
    }
    publications {
        publications {
            create<MavenPublication>("maven") {
                groupId = project.group as String
                artifactId = project.name
                version = project.version as String
                from(components["java"])
            }
        }
    }
}
