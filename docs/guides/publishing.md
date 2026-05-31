# Publishing to Maven Central

This project is wired to publish all 13 modules to **Maven Central** through the
**Central Portal** (the `central.sonatype.com` flow that replaced OSSRH). The
Gradle build already produces, for every module, a `jar` + `sources` + `javadoc`
+ `.pom` (and `.asc` signatures once a key is configured) and bundles them for
upload — you only need to supply an account, a verified namespace, and a signing
key.

## The namespace

The build publishes under **`io.github.altspacetg`** — the group id in the root
`build.gradle.kts`, derived from the GitHub account `AltSpaceTG`. The
`io.github.<user>` form is verified for free by proving you own the GitHub
account; no domain is required. Publishing under a different account/org? Change
`group` in the root build.

## One-time setup

### 1. Central Portal account + namespace

1. Sign in at [central.sonatype.com](https://central.sonatype.com/) with GitHub.
2. **Add namespace** → `io.github.altspacetg`. For `io.github.*` the Portal asks
   you to create a public GitHub repository whose name is the verification code
   it shows. Create it, click **Verify**.
3. **Account → Generate User Token** → you get a username/password pair. These are
   your `mavenCentralUsername` / `mavenCentralPassword`.

### 2. A PGP signing key

Central requires every artifact to be signed.

```bash
gpg --full-generate-key                                   # RSA 4096, no expiry is fine
gpg --list-secret-keys --keyid-format=long                # find the key id
gpg --keyserver keyserver.ubuntu.com --send-keys <KEY_ID> # publish the public half
gpg --armor --export-secret-keys <KEY_ID> > fom-signing-key.asc   # export the secret half
```

### 3. Put the credentials where Gradle finds them

Never commit secrets. Use your **user** Gradle properties
(`~/.gradle/gradle.properties`), which live outside the repo:

```properties
mavenCentralUsername=<portal token username>
mavenCentralPassword=<portal token password>

# inline the armored key with literal \n for newlines:
signingInMemoryKey=-----BEGIN PGP PRIVATE KEY BLOCK-----\n...\n-----END...
signingInMemoryKeyPassword=<key passphrase>
```

`gradle.properties.example` in the repo lists the same keys as a reminder. The
root `gradle.properties`, plus `*.asc`/`*.gpg`/`secrets.properties`, are
git-ignored so a real key can't be committed by accident. In CI prefer
environment variables: `ORG_GRADLE_PROJECT_mavenCentralUsername`,
`ORG_GRADLE_PROJECT_signingInMemoryKey`, etc.

## Set a release version

Central **rejects `-SNAPSHOT`** versions and won't accept the same version twice.
Set a real version in the root `build.gradle.kts`:

```kotlin
version = "0.1.0"   // was 0.1.0-SNAPSHOT
```

## Build the upload bundle

```bash
./gradlew clean build            # green build first (tests, -Werror, all modules)
./gradlew centralBundle          # stages + signs every module, zips one bundle
```

This produces **`build/central-bundle.zip`** in the Central Portal's required
Maven layout — `io/github/altspacetg/<module>/<version>/…` with `.jar`,
`-sources.jar`, `-javadoc.jar`, `.pom`, the `.md5/.sha*` checksums, and `.asc`
signatures for each (signatures appear only when a signing key is configured).
The test-only `fom-core` test-fixtures artifact is deliberately excluded.

!!! note "What `centralBundle` does"
    Each library module publishes into a shared `build/staging-deploy` directory
    via Gradle's built-in `maven-publish`; `signing` attaches `.asc` files when a
    key is configured; the root `centralBundle` task zips that directory. No
    third-party Gradle plugin is involved, so it stays compatible with the
    project's Gradle 9 toolchain.

## Upload & release

1. In the Portal: **Publish → Upload a Deployment** and select
   `build/central-bundle.zip`.
2. The Portal validates the bundle (POM completeness, signatures, sources +
   javadoc present). Fix anything it reports and re-upload.
3. When validation passes, **Publish** the deployment. First-time releases under
   a new namespace can take a little while to propagate to `repo1.maven.org` and
   search.

After release, consumers depend on it exactly as in
[installation](../getting-started/installation.md), with your release version.

## Local testing without Central

`./gradlew publishToMavenLocal` installs every module into `~/.m2`, so another
local project can resolve `io.github.altspacetg:fom-core:<version>` before you
ever touch the Portal.

## Checklist

- [ ] Namespace `io.github.altspacetg` verified in the Central Portal.
- [ ] Portal user token in `~/.gradle/gradle.properties`.
- [ ] PGP key generated, public half on a keyserver, secret half configured.
- [ ] `version` set to a non-`SNAPSHOT` value (bumped for each release).
- [ ] `./gradlew clean build` green.
- [ ] `./gradlew centralBundle` → `build/central-bundle.zip` produced with `.asc`
      signatures.
- [ ] Bundle uploaded and published in the Portal.
