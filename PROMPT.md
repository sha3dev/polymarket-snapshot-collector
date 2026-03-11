Read these files before making any implementation changes:

- `AGENTS.md`
- `ai/contract.json`
- `ai/rules.md`
- `prompts/init-contract.md`
- the assistant-specific adapter in `ai/`

Your job is to implement the requested behavior in the scaffold under `src/` and `test/` following the rules in `ai/rules.md` and `prompts/init-contract.md`.

## Package Specification

- Goal:
- Public API:
- Runtime constraints:
- Required dependencies:
- Feature requirements:

## Non-Negotiables

- You MUST execute `npm run check` yourself before finishing.
- If `npm run check` fails, you MUST fix the issues and rerun it until it passes.
- You MUST implement the task without editing managed files unless this is a standards update.

## Implementation Request

Complete this section before sending the prompt to your LLM.
Describe the behavior you want to implement, the expected public API, any runtime constraints, and any non-goals.

Task:

Este servicio tiene como misión persistir en una base de datos clickhouse los snapshots generados por
la libreria @sha3/polymarket-snapshot.

La idea es que tendremos tres tablas en clickhouse: "market", "snapshot_crypto" y "snapshot_polymarket".

Este servicio ira persistiendo los snapshots en las tablas correspondientes cada vez que se genere un snapshot.

Los snapshots, se parten en dos tablas: "snapshot_crypto" y "snapshot_polymarket", el objetivo es que para generar un snapshot completo
tendremos que hacer JOIN de las dos tablas (por asset:btc, eth, sol, xrp) y por fecha. Como los snapshots se generan siempre en momentos
concretos (cada 500ms, es decir, en los momentos 00:00:00.000, 00:00:00.500, 00:00:01.000, etc.) podemos hacer esa join.

Ademas de persistir los snapshots, este servicio tambien debe exponer una API para consultar los snapshots generados. Esta api tendrá los siguientes mñetodos:

    * Un endpoint que recibirá un asset (btc, eth, sol, xrp) y un window (5m, 15m) y me devolverá un listado con todos los mercados de ese asset en ese window. Tambien acepta opcionalmente, un from_date
      si le paso el from_Date solo me devolverá mercados que han comenzado en esa fecha o despues. La info que tiene que devovler para cada mercado es: slug, window, asset, priceToBeat, fecha inicio, fecha fin

    * Un endpoint que recibirá un slug de mercado, y me devolverá todos los snapshots de ese mercado en el window correspondiente (OJO, aqui hay que hacer join entre crypto y polymarket). Si el slug
      pertenecea a un mercado de 5m, como el intervalMs es de 500ms deberiamos tener un maximo de 600 snapshots, para 15m deberiamos tener un maximo de 1800 snapshots.

    *  Los snapshot que devovlemos, deben seguir la misma estructura que marca la libreria @sha3/polymarket-snapshot

    * La api no requiere autenticacion de ningun tipo, se usará solo de forma interna, no estará expuesta al exteruior.
