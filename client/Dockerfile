FROM node:current-alpine AS scaffold

WORKDIR /usr/src

RUN npx @react-native-community/cli@latest init app --version latest --title HyperProxy --package-name com.slugfiller.hyperproxy --skip-install

CMD ["echo", "Done"]

# Do NOT use Alpine. It is not compatible with React Native
FROM node:current-bookworm AS package-lock

# Java is not needed by package-lock, but it is used by all of its dependents. Install it before copying any files
RUN apt-get update && apt-get install -y openjdk-17-jdk && apt-get clean && rm -rf /var/lib/apt/lists/*

# Also install android SDK
WORKDIR /opt/android-sdk-linux/cmdline-tools
ENV ANDROID_HOME=/opt/android-sdk-linux
RUN wget https://dl.google.com/android/repository/commandlinetools-linux-13114758_latest.zip -O tools.zip \
  && unzip tools.zip \
  && rm tools.zip \
  && mv ./cmdline-tools ./latest \
  && (yes | ./latest/bin/sdkmanager --licenses)

WORKDIR /usr/src/app

COPY ./app/package.json ./app/package-lock.jso[n] /usr/src/app/

RUN npm install

WORKDIR /usr/src/app/backend/

COPY ./app/backend/package.json ./app/backend/package-lock.jso[n] /usr/src/app/backend/

RUN npm install

CMD ["echo", "Done"]

FROM package-lock AS metro-build

COPY ./app/android /usr/src/app/android/

COPY ./app/index.js /usr/src/app/

COPY ./app/specs /usr/src/app/specs

WORKDIR /usr/src/app/android

RUN ./gradlew --build-cache --no-daemon assembleDebug

FROM package-lock AS metro

WORKDIR /usr/src/app

COPY ./app /usr/src/app/

RUN npx bare-pack --target android --linked --out ./backend.bundle.mjs backend/backend.mjs

RUN npx tsc --noEmit

RUN npm run lint

CMD ["npm", "run", "start"]

FROM package-lock AS keygen

WORKDIR /usr/src/app/android/app

RUN keytool -genkeypair -v -storetype PKCS12 -keystore release.keystore -storepass android -alias androidreleasekey -keypass android -keyalg RSA -keysize 2048 -validity 10000 -dname "CN=Unknown, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown"

FROM package-lock AS release

WORKDIR /usr/src/app/android

COPY ./app/android/gradle /usr/src/app/android/gradle/
COPY ./app/android/build.gradle ./app/android/gradle.properties ./app/android/gradlew ./app/android/settings.gradle /usr/src/app/android
COPY ./app/android/app/build.gradle ./app/android/app/release.keystore ./app/android/app/proguard-rules.pro /usr/src/app/android/app/

RUN ./gradlew --build-cache --no-daemon

COPY ./app/android/app/src/main/AndroidManifest.xml /usr/src/app/android/app/src/main/AndroidManifest.xml

COPY ./app/specs /usr/src/app/specs

RUN ./gradlew --build-cache --no-daemon :app:mergeReleaseNativeLibs

COPY ./app /usr/src/app/

WORKDIR /usr/src/app

RUN npx bare-pack --target android --linked --out ./backend.bundle.mjs backend/backend.mjs

WORKDIR /usr/src/app/android

RUN ./gradlew --build-cache --no-daemon assembleRelease
