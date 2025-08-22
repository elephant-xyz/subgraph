# Property Data Subgraph

A subgraph for indexing PropertyDataConsensus contract events on Polygon mainnet.

## Contract Details
- **Contract Address:** `0x525E59e4DE2B51f52B9e30745a513E407652AB7c`
- **Network:** Polygon (Matic)
- **Event:** `DataSubmitted`

## Setup

1. **Install dependencies:**
   ```bash
   npm install
   ```

2. **Generate code from schema and ABI:**
   ```bash
   npm run codegen
   ```

3. **Build the subgraph:**
   ```bash
   npm run build
   ```

4. **Depoly on alchemy:**
    ```bash
    graph deploy property-data-subgraph \
    --version-label The-version \
    --node https://subgraphs.alchemy.com/api/subgraphs/deploy \
    --deploy-key P9olXkeTVfE2H \
    --ipfs https://ipfs.satsuma.xyz

    ```
## Deployment

### Option 1: Deploy to The Graph Studio (Recommended)

1. **Go to [The Graph Studio](https://thegraph.com/studio/)**

2. **Create a new subgraph** and get your deployment key

3. **Authenticate with your deployment key:**
   ```bash
   graph auth YOUR_DEPLOY_KEY_HERE
   ```

4. **Deploy to Studio:**
   ```bash
   npm run deploy
   ```

### Option 2: Deploy to Hosted Service (Legacy)

1. **Authenticate with access token:**
   ```bash
   graph auth --product hosted-service YOUR_ACCESS_TOKEN
   ```

2. **Deploy:**
   ```bash
   graph deploy --product hosted-service YOUR_GITHUB_USERNAME/property-data-subgraph
   ```

### Option 3: Local Development

1. **Start local Graph node** (requires Docker):
   ```bash
   # Follow The Graph docs to set up local node
   ```

2. **Create local subgraph:**
   ```bash
   npm run create-local
   ```

3. **Deploy locally:**
   ```bash
   npm run deploy-local
   ```

## GraphQL Queries

Once deployed, you can query your subgraph using GraphQL. Here are some example queries:

### Get Latest Properties
```graphql
{
  properties(first: 10, orderBy: timestamp, orderDirection: desc) {
    id
    propertyHash
    dataGroupHash
    owner
    submitter
    dataHash
    timestamp
    blockNumber
    transactionHash
  }
}
```

### Get Properties by Submitter
```graphql
{
  properties(where: { submitter: "0x1234567890123456789012345678901234567890" }) {
    id
    propertyHash
    dataGroupHash
    dataHash
    timestamp
  }
}
```

### Get Properties by Property Hash
```graphql
{
  properties(where: { propertyHash: "0xabcdef..." }) {
    id
    dataGroupHash
    submitter
    dataHash
    timestamp
    blockNumber
  }
}
```

### Search with Pagination
```graphql
{
  properties(
    first: 5
    skip: 10
    orderBy: timestamp
    orderDirection: desc
    where: { 
      timestamp_gt: "1609459200" 
    }
  ) {
    id
    propertyHash
    submitter
    timestamp
  }
}
```

## File Structure

```
├── schema.graphql          # GraphQL schema defining Property entity
├── subgraph.yaml          # Subgraph configuration
├── src/
│   └── mapping.ts         # Event handlers (handleDataSubmitted)
├── abis/
│   └── PropertyDataConsensus.json  # Contract ABI
└── package.json           # Dependencies and scripts
```

## Entity Schema

The `Property` entity includes:
- `id`: Unique identifier (transaction hash + log index)
- `propertyHash`: Property identifier from the event
- `dataGroupHash`: Data group identifier
- `owner`: Address of the property owner (set to submitter)
- `submitter`: Address that submitted the data
- `dataHash`: Hash of the submitted data
- `timestamp`: Block timestamp when the event was emitted
- `blockNumber`: Block number of the transaction
- `transactionHash`: Transaction hash

## Development

To modify the subgraph:

1. Update `schema.graphql` for entity changes
2. Update `src/mapping.ts` for new event handling logic
3. Run `npm run codegen` to regenerate types
4. Run `npm run build` to build
5. Deploy with your preferred method

## Troubleshooting

- **Build failures**: Ensure all dependencies are installed and ABI file exists
- **Deployment failures**: Check authentication and subgraph name
- **Query failures**: Verify the subgraph is fully synced before querying
- **Missing events**: Check the `startBlock` in `subgraph.yaml` is before your first transaction