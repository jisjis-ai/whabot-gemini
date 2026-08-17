FROM node:20-slim

WORKDIR /app

# Copiar arquivos de dependência
COPY package*.json ./

# Instalar dependências de produção
RUN npm ci --only=production

# Copiar o restante dos arquivos da aplicação
COPY . .

# Expôr a porta que o Render utiliza
EXPOSE 3000

# Variáveis de ambiente padrão para persistência no Render
ENV PORT=3000
ENV DATA_DIR=/var/data

# Iniciar o bot
CMD ["npm", "start"]
