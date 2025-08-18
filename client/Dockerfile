FROM node:current-alpine AS scaffold

WORKDIR /usr/src

RUN npx @react-native-community/cli@latest init app --version latest --title HyperProxy --package-name com.slugfiller.hyperproxy --skip-install

CMD ["echo", "Done"]

FROM node:current-alpine AS package-lock

WORKDIR /usr/src/app

COPY ./app/package.json ./app/package-lock.jso[n] /usr/src/app/

RUN npm install

CMD ["echo", "Done"]
