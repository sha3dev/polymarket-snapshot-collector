module.exports = {
  apps: [
    {
      name: "@sha3/polymarket-snapshot-collector",
      script: "node",
      args: "--import tsx src/main.ts",
      env: { NODE_ENV: "production" },
    },
    {
      name: "@sha3/polymarket-snapshot-collector-migrate",
      script: "node",
      args: "--import tsx src/main.ts --mode=migrate",
      env: { NODE_ENV: "production" },
    },
  ],
};
