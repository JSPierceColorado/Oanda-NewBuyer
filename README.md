{
  "$schema": "https://railway.com/railway.schema.json",
  "build": {
    "builder": "DOCKERFILE"
  },
  "deploy": {
    "startCommand": "python -m src.main",
    "restartPolicyType": "ALWAYS",
    "restartPolicyMaxRetries": 10
  }
}
