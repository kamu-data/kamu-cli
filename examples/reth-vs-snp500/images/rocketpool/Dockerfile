FROM node:18-slim

# Tini
ADD https://github.com/krallin/tini/releases/download/v0.19.0/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

# Deps
WORKDIR /opt/odf
COPY package*.json ./
RUN npm install

# App
COPY . .
RUN npm run build

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["node", "dist/app.js"]
