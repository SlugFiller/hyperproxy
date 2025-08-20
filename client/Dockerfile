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

CMD ["echo", "Done"]

FROM package-lock AS metro-build

COPY ./app/android /usr/src/app/android/

COPY ./app/index.js /usr/src/app/

WORKDIR /usr/src/app/android

RUN ./gradlew --build-cache --no-daemon assembleDebug

FROM package-lock AS metro

WORKDIR /usr/src/app

COPY ./app /usr/src/app/

RUN npm run lint

CMD ["npm", "run", "start"]
