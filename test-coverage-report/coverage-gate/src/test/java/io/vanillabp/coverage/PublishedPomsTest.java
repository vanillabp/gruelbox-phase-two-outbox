package io.vanillabp.coverage;

import java.util.Set;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vanillabp.integration.test.utils.PublishedPoms;
import io.vanillabp.integration.test.utils.SuppressOutputExtension;

/**
 * What the POMs of this repository hand an application. They hand it no tool which only
 * translates our source, and no property name where a version belongs.
 * <p>
 * Lombok and the processors of Spring Boot and Quarkus are used while javac runs. They have
 * nothing left to do once the class file exists. An application asked for a workflow
 * engine, not for them. Every jar it did not ask for is one more thing to scan, to ship and
 * to answer a CVE report about. The scope which says that is {@code provided}. It puts the
 * jar on our own compile path and hands it to nobody.
 * <p>
 * The second check reads the versions in those POMs. A version written as
 * {@code ${some.version}} resolves in our own build, where the file holding the value is in
 * the reactor. For an application, it stops resolving on the day that file stops defining
 * the property. A published snapshot cannot keep the parent build it went out with. Maven
 * resolves it against the newest build of that parent. So the value has to stand in the
 * file which names it. On 2026-09-27 this turned every Quarkus blueprint red, while the
 * build of the platform itself stayed green.
 * <p>
 * The rules of the checks live in {@link PublishedPoms}, in the module 'test-utils' of the
 * platform. Every repository of VanillaBP can make these mistakes, and all make them in the
 * same way. So the rules live in one place, and each repository calls them. A copy in each
 * repository drifts apart, and these checks are only worth something if they still run in
 * two years. This test is the caller. It names the file this repository publishes and the
 * tools of this build.
 * <p>
 * This repository publishes its POMs as they are. So an application reads the same file a
 * reviewer reads, and the checks read it too.
 * <p>
 * It is a gate in the module which already gates this repository as a whole. That module is
 * the last of the reactor, so it reads what every other module has published by then.
 */
@ExtendWith(SuppressOutputExtension.class)
public class PublishedPomsTest {

  /**
   * The tools of this build, each as {@code groupId:artifactId}. When this repository starts
   * using a new tool, it belongs in this list, because nothing else knows that it is a tool.
   * The list names more than the build uses today, so the usual next one is covered too.
   */
  private static final Set<String> TOOLS_OF_THIS_BUILD = Set
      .of(
          "org.projectlombok:lombok",
          "io.quarkus:quarkus-extension-processor",
          "org.springframework.boot:spring-boot-autoconfigure-processor",
          "org.mapstruct:mapstruct-processor",
          "io.vanillabp:vanillabp-mapstruct-fluent-accessors");

  @Test
  @DisplayName("no POM of this repository hands an application a tool of the build")
  public void noPomHandsAnApplicationAToolOfTheBuild() {

    PublishedPoms
        .ofTheRepositoryUnderTest(PublishedPoms.THE_SOURCE_POM)
        .handAnApplicationNoToolOfTheBuild(TOOLS_OF_THIS_BUILD);

  }

  @Test
  @DisplayName("no POM of this repository hands an application a property instead of a value")
  public void noPomHandsAnApplicationAPropertyInsteadOfAValue() {

    PublishedPoms
        .ofTheRepositoryUnderTest(PublishedPoms.THE_SOURCE_POM)
        .handAnApplicationNoPropertyInsteadOfAValue();

  }

}
