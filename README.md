# Roke.to mass streaming creation script

## Usage guide

1. Install `node` version `>=16` and `yarn` version `1`.
2. Install node packages:

```bash
yarn
```

3. Log in NEAR wallet:

```bash
yarn near login
```

Or, if you have `near-cli` installed globally:

```bash
near login
```

This command uses `testnet` network by default. If you want to log in NEAR wallet in `mainnet`, set environment variable by following [this instruction](https://docs.near.org/docs/tools/near-cli#network-selection).

4. Create a file with comma-separated values (check [this example](example/example.csv) for reference) in `receiverAccountId,amount` format. Amount is specified in regular tokens, not yocto.

5. Run the script specifying all the desired options:

```bash
yarn create-roketo-mass-streaming [options] [filename]
```

## Required options

* `csv`: relative path to a csv-file with receivers and amounts (can also be specified as a default option);
* `speed`: shared speed for all created streams in yocto tokens per second (mutually exclusive with `endTimestamp` option);
* `endTimestamp`: Unix timestamp with shared end date for all created streams (mutually exclusive with `speed` option);
* `senderAccountId`: your account ID from step 3 of usage guide;
* `tokenAccountId`: FT account id of a token you want to create mass streaming with.

## Optional options
* `network`: `mainnet` or `testnet` (default);
* `locked`: check this flag to prevent created streams from being paused or stopped prematurely;
* `cliffTimestamp`: Unix timestamp with shared cliff date for all created streams;
* `dryRun`: check this flag for a dry run without any valuables transfer;
* `delimiter`: delimiter in csv (default value is `,`);
* `skipExistenceChecks`: check this flag to allow script to create streams to receivers, which weren't explicitly created.

## Cache

The script creates a cache file ending with `.cache.json` extension near a specified csv-file for caching accounts existence, FT storage registration and created streams. After all streams are successfully created, the cache file can be safely deleted.

## Example

E.g. if `lebedev.testnet` is to dry run locked streams creation with token `dai.fakes.testnet` in `testnet` ending on December 31st with cliff date set to September 1st:

Timestamp for end date:

```js
new Date('12/31/2022, 11:59:59 PM').getTime() // 1672509599000
```

Timestamp for cliff date:

```js
new Date('09/01/2022, 11:59:59 PM').getTime() // 1662055199000
```

The final command:

```bash
yarn create-roketo-mass-streaming --endTimestamp=1672509599000 --cliffTimestamp=1662055199000 --senderAccountId=lebedev.testnet --tokenAccountId=dai.fakes.testnet --dryRun --locked --delimiter=';' example/example.csv
```

## Support

Tested on Ubuntu Linux.

If you experience any issued, you can write to https://t.me/angly or https://t.me/dcversus for support.