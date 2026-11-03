FROM mcr.microsoft.com/playwright:v1.64.1-noble
# kubectl + helm: agent journeys install the agent chart exactly as a user would.
ARG KUBECTL_VERSION=v1.33.5
ARG HELM_VERSION=v3.19.0
RUN arch=$(dpkg --print-architecture) \
 && curl -fsSL -o /usr/local/bin/kubectl "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${arch}/kubectl" \
 && chmod +x /usr/local/bin/kubectl \
 && curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-${arch}.tar.gz" | tar -xz -C /tmp \
 && mv "/tmp/linux-${arch}/helm" /usr/local/bin/helm && rm -rf "/tmp/linux-${arch}"
WORKDIR /e2e
COPY package.json package-lock.json ./
RUN npm ci --silent
COPY . .
ENTRYPOINT ["npx", "playwright", "test"]
