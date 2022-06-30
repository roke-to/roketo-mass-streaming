const nearAPI = require('near-api-js');
const commandLineArgs = require('command-line-args');
const fs = require('fs');
const BigNumber = require('bignumber.js');
const cliProgress = require('cli-progress');
const readline = require('readline');
const dateFns = require('date-fns');
const retry = require('async-retry');

function getCLIParams() {
  class FileDetails {
    constructor(filename) {
      this.filename = filename
      this.exists = fs.existsSync(filename)
    }
  }

  const optionDefinitions = [
    { name: 'csv', type: (filename) => new FileDetails(filename), defaultOption: true },
    { name: 'cliffTimestamp', type: Number },
    { name: 'speed', type: (value) => value && new BigNumber(value) },
    { name: 'endTimestamp', type: Number },
    { name: 'network', type: String, defaultValue: 'testnet' },
    { name: 'locked', type: Boolean, defaultValue: false },
    { name: 'senderAccountId', type: String },
    { name: 'tokenAccountId', type: String },
    { name: 'dryRun', type: Boolean, defaultValue: false },
    { name: 'delimiter', type: String, defaultValue: ',' },
  ];

  return commandLineArgs(optionDefinitions);
}

const options = getCLIParams();

function checkCLIParams(options) {
  if (!options.csv) {
    console.log(`Please specify [csv] option with a path for a csv-file containing receiverId and amount per row. See example/example.csv for a reference.`);
    process.exit(1);
  }

  if (!options.csv.exists) {
    console.log(`Error in [csv] argument: can't find a file at path ${options.csv.filename}.`);
    process.exit(1);
  }

  if (!options.speed && !options.endTimestamp) {
    console.log(`Please specify either [speed] option for all streams (in tokens per second), or [endTimestamp] with Unix timestamp for when all streams should end.`);
    process.exit(1);
  }

  if (options.speed && options.endTimestamp) {
    console.log(`Please use either [speed] option, or [endTimestamp], as they are mutually exclusive.`);
    process.exit(1);
  }

  if (options.network !== 'testnet' && options.network !== 'mainnet') {
    console.log(`Please specify either "mainnet" value for [network] option, or "testnet" (default value).`);
    process.exit(1);
  }

  if (!options.senderAccountId) {
    console.log(`Please specify a sender near account ID in [senderAccountId] option.`);
    process.exit(1);
  }

  if (!options.tokenAccountId) {
    console.log(`Please specify an FT token account ID in [tokenAccountId] option.`);
    process.exit(1);
  }
}

function getConfig(network) {
  const configMap = {
    mainnet: {
      roketoContractName: 'streaming.r-v2.near',
      nearConfig: {
        networkId: 'mainnet',
        nodeUrl: 'https://rpc.mainnet.near.org',
        walletUrl: 'https://wallet.near.org',
      },
    },
    testnet: {
      roketoContractName: 'streaming-r-v2.dcversus.testnet',
      nearConfig: {
        networkId: 'testnet',
        nodeUrl: 'https://rpc.testnet.near.org',
        walletUrl: 'https://wallet.testnet.near.org',
      },
    },
  };

  return configMap[network];
}

function getNearInstance(config) {
  const keyStore = new nearAPI.keyStores.UnencryptedFileSystemKeyStore(
    `${process.env.HOME}/.near-credentials/`
  );

  return nearAPI.connect({
    keyStore,
    ...config,
  });
}

async function checkSenderAccess(senderAccount) {
  const keys = await senderAccount.findAccessKey();

  if (keys?.accessKey?.permission !== 'FullAccess') {
    console.log(`Can't find full access key in $HOME/.near-credentials. Please check if [senderAccountId] option is correct or try logging in with "yarn near login".`);
    process.exit(1);
  }

  console.log(`✔️ ${senderAccount.accountId} access checked.`);
}

function checkCSVCorrectness(lines, filename) {
  const linesCorrectness =
    lines.map((line) => {
      const parts = line.split(options.delimiter);
      const [, amount] = parts;

      return !line || (parts.length === 2 && amount && !Number.isNaN(Number(amount)));
    }) ?? [];

  const incorrectLinesIndices = linesCorrectness
    .map((isCorrect, index) => (isCorrect ? null : index + 1))
    .filter((lineNumber) => typeof lineNumber === 'number');

  if (incorrectLinesIndices.length > 0) {
    console.log([
      `Error in csv-file ${filename}, incorrect format on lines:`,
      incorrectLinesIndices.map((index) => `${index}: ${lines[index - 1]}`),
      `Expected format is "accountId,123". Check example/example.csv for reference.`,
    ].join('\n'));
    process.exit(1);
  }

  console.log(`✔️ ${filename} format checked.`);
}

function checkReceiversCorrectness(lines, senderAccountId) {
  const linesCorrectness = lines.map((line) => {
    const [receiverAccountId] = line.split(options.delimiter);

    return receiverAccountId !== senderAccountId;
  });

  const incorrectLinesIndices = linesCorrectness
    .map((isCorrect, index) => (isCorrect ? null : index + 1))
    .filter((lineNumber) => typeof lineNumber === 'number');

  if (incorrectLinesIndices.length > 0) {
    console.log(`Receivers on some lines are the same as [senderAccountId] "${senderAccountId}", lines: ${incorrectLinesIndices.join(', ')}. Aborting...`);
    process.exit(1);
  }

  console.log(`✔️ No ${senderAccountId} being among receivers checked.`);
}

async function checkAccountIdExistence(accountId, near) {
  try {
    const result = await near.connection.provider.query({
      request_type: 'view_account',
      finality: 'final',
      account_id: accountId,
    });
    return Boolean(result);
  } catch (e) {
    return false;
  }
}

async function checkReceiversExistence(lines, filename, near) {
  const receivers =
    lines.map((line) => {
      const [receiver] = line.split(options.delimiter);

      return receiver;
    });

  const receiversLinesMap = receivers.reduce(
    (map, receiver, index) => {
      if (receiver) {
        if (!map[receiver]) {
          map[receiver] = [];
        }

        map[receiver].push(index + 1);
      }

      return map;
    },
    {}
  );

  const uniqueReceivers = Object.keys(receiversLinesMap);

  const bar = new cliProgress.MultiBar({
    stopOnComplete: true,
    forceRedraw: true,
    clearOnComplete: true,
  }, {
    ...cliProgress.Presets.shades_classic,
    format: 'Checking receivers\' existence:' + cliProgress.Presets.shades_classic.format,
  }).create(uniqueReceivers.length, 0);

  let haveNonExistent = false;

  const existingKey = 'existing';

  const existsCache = (() => {
    try {
      const cacheString = fs.readFileSync(`${filename}.cache.json`, { encoding: 'utf-8' })
      const cache = JSON.parse(cacheString);
      return cache[existingKey] ?? [];
    } catch {
      return [];
    }
  })();

  await Promise.all(uniqueReceivers.map(async (accountId) => {

    if (existsCache.includes(accountId)) {
      bar.increment();

      return;
    }

    const exists = await checkAccountIdExistence(accountId, near);

    if (!exists) {
      if (!haveNonExistent) {
        haveNonExistent = true;
        readline.cursorTo(process.stderr, 0, null);
        readline.clearLine(process.stderr, 1);
        console.log('Non-existent receivers are specified on the following lines:');
      }

      const lineNumbers = receiversLinesMap[accountId];

      readline.cursorTo(process.stderr, 0, null);
      readline.clearLine(process.stderr, 1);
      console.log(`${accountId} on line${lineNumbers.length > 1 ? 's:' : ''} ${lineNumbers.join(', ')}.`);
    } else {
      const cache = (() => {
        try {
          const cacheString = fs.readFileSync(`${filename}.cache.json`, { encoding: 'utf-8' })
          return JSON.parse(cacheString);
        } catch {
          return {};
        }
      })();

      if (cache[existingKey]) {
        cache[existingKey].push(accountId);
      } else {
        cache[existingKey] = [accountId];
      }

      fs.writeFileSync(`${filename}.cache.json`, JSON.stringify(cache, null, 2));
    }

    bar.increment();
  }));

  if (haveNonExistent) {
    console.log('Aborting...');
    process.exit(1);
  }

  await new Promise(function giveBarTimeToEraseItself(resolve) { setTimeout(resolve, 100) });

  console.log(`✔️ All receiver accounts existence checked.`);
}

async function getAccountIdsWithoutStorageBalancesSet(senderAccountId, lines, filename, tokenContract) {
  const receivers =
    lines.map((line) => {
      const [receiver] = line.split(options.delimiter);

      return receiver;
    });

  const receiversLinesMap = receivers.reduce(
    (map, receiver, index) => {
      if (receiver) {
        if (!map[receiver]) {
          map[receiver] = [];
        }

        map[receiver].push(index + 1);
      }

      return map;
    },
    {}
  );

  const uniqueReceivers = Object.keys(receiversLinesMap);

  const uniqueAccounts = [senderAccountId, ...uniqueReceivers];

  const bar = new cliProgress.MultiBar({
    stopOnComplete: true,
    forceRedraw: true,
    clearOnComplete: true,
  }, {
    ...cliProgress.Presets.shades_classic,
    format: 'Checking receivers\' FT storage balances:' + cliProgress.Presets.shades_classic.format,
  }).create(uniqueAccounts.length, 0);

  const accountIdsWithoutStorageBalancesSet = new Set();

  const withStorageBalanceKey = `withStorageBalance_${tokenContract.contractId}`;

  const withStorageBalanceCache = (() => {
    try {
      const cacheString = fs.readFileSync(`${filename}.cache.json`, { encoding: 'utf-8' })
      const cache = JSON.parse(cacheString);
      return cache[withStorageBalanceKey] ?? [];
    } catch {
      return [];
    }
  })();

  await Promise.all(uniqueAccounts.map(async (accountId) => {
    if (withStorageBalanceCache.includes(accountId)) {
      bar.increment();

      return;
    }

    const storage = await tokenContract.storage_balance_of({ account_id: accountId });
    const hasStorageBalance = storage && storage.total !== '0';

    if (!hasStorageBalance) {
      accountIdsWithoutStorageBalancesSet.add(accountId);
    } else {
      const cache = (() => {
        try {
          const cacheString = fs.readFileSync(`${filename}.cache.json`, { encoding: 'utf-8' })
          return JSON.parse(cacheString);
        } catch {
          return {};
        }
      })();

      if (cache[withStorageBalanceKey]) {
        cache[withStorageBalanceKey].push(accountId);
      } else {
        cache[withStorageBalanceKey] = [accountId];
      }

      fs.writeFileSync(`${filename}.cache.json`, JSON.stringify(cache, null, 2));
    }

    bar.increment();
  }));

  return accountIdsWithoutStorageBalancesSet;
}

async function checkIfEnoughNEARs(senderAccount, accountIdsWithoutStorageBalancesSet) {
  const senderAccountState = await senderAccount.state();

  const ftStorageRegistrationFeeNear = new BigNumber(accountIdsWithoutStorageBalancesSet.size).multipliedBy(nearAPI.utils.format.parseNearAmount('0.00125'));

  if (ftStorageRegistrationFeeNear.isGreaterThan(senderAccountState.amount)) {
    const senderNearBalance = nearAPI.utils.format.parseNearAmount(senderAccountState.amount);
    const diff = nearAPI.utils.format.parseNearAmount(ftStorageRegistrationFeeNear.toFixed());

    console.log([
      `Not enough NEAR on ${senderAccount.accountId} account for covering receivers' FT storage registration.`,
      `Current balance: ${senderNearBalance} NEAR.`,
      `Required balance: ${ftStorageRegistrationFeeNear.toFixed()} NEAR.`,
      `Please add ${diff} more NEAR to ${senderAccount.accountId} before proceeding.`,
      `Aborting...`,
    ].join('\n'));
    process.exit(1);
  }

  if (ftStorageRegistrationFeeNear.isGreaterThan(0)) {
    console.log(`✔️ There're enough NEARs on ${senderAccount.accountId} to cover receivers' FT storage registration.`);
  }
}

async function checkIfEnoughFTs(tokenContract, ftMetadata, senderAccount, lines) {
  const senderFTBalanceRaw = await tokenContract.ft_balance_of({ account_id: senderAccount.accountId });

  const senderFTBalance = new BigNumber(senderFTBalanceRaw).dividedBy(new BigNumber(10).exponentiatedBy(ftMetadata.decimals));

  const amounts = lines.filter(Boolean).map((line) => line.split(options.delimiter)[1]);

  const requiredFTBalance = amounts.reduce((sum, amount) => sum.plus(amount), new BigNumber(0));

  if (requiredFTBalance.isGreaterThan(senderFTBalance)) {
    console.log([
      `Not enough ${ftMetadata.symbol} on ${senderAccount.accountId} account to create all streams.`,
      `Current balance: ${senderFTBalance.toFixed()} ${ftMetadata.symbol}.`,
      `Required balance: ${requiredFTBalance.toFixed()} ${ftMetadata.symbol}.`,
      `Please add ${requiredFTBalance.minus(senderFTBalance).toFixed()} more ${ftMetadata.symbol}s to ${senderAccount.accountId} before proceeding.`,
      `Aborting...`,
    ].join('\n'));
    process.exit(1);
  }

  console.log(`✔️ There're enough ${ftMetadata.symbol}s on ${senderAccount.accountId} to create all streams.`);
}

function printSummary(lines, options, ftMetadata) {
  console.log('\nSummary:');

  const receiverAndAmountPairs = lines.filter(Boolean).map((line) => line.split(options.delimiter));

  const amounts = receiverAndAmountPairs.map(([, amount]) => amount);

  const uniqueReceiversCount = Array.from(new Set(receiverAndAmountPairs.map(([receiver]) => receiver))).length;

  const MILLISECONDS_IN_SECOND = 1000;

  const minEndTimestamp = options.endTimestamp ?? BigNumber.min(...amounts)
    .multipliedBy(new BigNumber(10).exponentiatedBy(ftMetadata.decimals))
    .multipliedBy(MILLISECONDS_IN_SECOND)
    .dividedToIntegerBy(options.speed)
    .plus(Date.now())
    .toNumber();

  console.log(`Streams to be created: ${receiverAndAmountPairs.length}.`);
  if (uniqueReceiversCount < receiverAndAmountPairs.length) {
    console.log(`Unique receivers: ${uniqueReceiversCount}, which is ${receiverAndAmountPairs.length - uniqueReceiversCount} less that streams count.`);
  }

  const MILLISECONDS_IN_FIVE_YEARS = 5 * 365 * 24 * 60 * 60 * 1000;

  if (options.endTimestamp) {
    if (Number.isNaN(options.endTimestamp) || options.endTimestamp < Date.now() || options.endTimestamp > Date.now() + MILLISECONDS_IN_FIVE_YEARS) {
      console.log(`${options.endTimestamp} is not a valid timestamp for [endTimestamp]. Expected a timestamp in the future no more that 5 years ahead.`);
      console.log('Exiting...');
      process.exit(1);
    }

    console.log(`All streams will end on ${dateFns.format(options.endTimestamp, 'PPPPpppp')} (${dateFns.formatDistanceToNow(options.endTimestamp, { addSuffix: true })})`);
  } else if (options.speed) {
    if (Number.isNaN(options.speed) || options.speed < 0) {
      console.log(`${options.speed} is not a valid value for [speed]. Expected a positive integer speed.`);
      console.log('Exiting...');
      process.exit(1);
    }

    const maxEndTimestamp = BigNumber.max(...amounts)
      .multipliedBy(new BigNumber(10).exponentiatedBy(ftMetadata.decimals))
      .multipliedBy(MILLISECONDS_IN_SECOND)
      .dividedToIntegerBy(options.speed)
      .plus(Date.now())
      .toNumber();

    const MAX_TIMESTAMP = 8640000000000000;

    if (minEndTimestamp > MAX_TIMESTAMP) {
      console.log(`${options.speed} is a too low value for [speed]. Expected a speed big enough so all streams end before Unix epoch.`);
      console.log('Exiting...');
      process.exit(1);
    }

    if (minEndTimestamp > Date.now() + MILLISECONDS_IN_FIVE_YEARS) {
      console.log(`${options.speed} is a too low value for [speed]. Expected a speed big enough so all streams end in no more than 5 years, and not ${dateFns.formatDistanceToNow(maxEndTimestamp, { addSuffix: true })}.`);
      console.log('Exiting...');
      process.exit(1);
    }

    console.log(`All streams will have the same speed ${options.speed} yocto per second.`);
    if (minEndTimestamp < maxEndTimestamp) {
      console.log(`The first stream will end on ${dateFns.format(minEndTimestamp, 'PPPPpppp')} (${dateFns.formatDistanceToNow(minEndTimestamp, { addSuffix: true })})`);
      console.log(`The last stream will end on ${dateFns.format(maxEndTimestamp, 'PPPPpppp')} (${dateFns.formatDistanceToNow(maxEndTimestamp, { addSuffix: true })})`);
    } else {
      console.log(`All streams will end on ${dateFns.format(minEndTimestamp, 'PPPPpppp')} (${dateFns.formatDistanceToNow(minEndTimestamp, { addSuffix: true })})`);
    }
  }

  if (options.cliffTimestamp) {
    if (Number.isNaN(options.cliffTimestamp) || options.cliffTimestamp < Date.now() || options.cliffTimestamp > minEndTimestamp) {
      console.log(`${options.cliffTimestamp} is not a valid timestamp for [cliffTimestamp]. Expected a timestamp in the future but not past the fastest stream.`);
      console.log('Exiting...');
      process.exit(1);
    }

    console.log(`Cliff period for all streams will be passed on ${dateFns.format(options.cliffTimestamp, 'PPPPpppp')} (${dateFns.formatDistanceToNow(options.cliffTimestamp, { addSuffix: true })})`);
  }

  if (options.locked) {
    console.log(`All streams will be created locked.`);
  }

  console.log();
}

async function createStorageDeposits(accountIdsWithoutStorageBalancesSet, senderAccount, tokenContract, filename) {
  if (accountIdsWithoutStorageBalancesSet.size === 0) {
    console.log(`✔️ No need to create FT storages.`);
    return;
  }

  const bar = new cliProgress.MultiBar({
    stopOnComplete: true,
    forceRedraw: true,
    clearOnComplete: true,
  }, {
    ...cliProgress.Presets.shades_classic,
    format: 'Creating FT storages:' + cliProgress.Presets.shades_classic.format,
  }).create(accountIdsWithoutStorageBalancesSet.size, 0);

  const accountIdsInTens = Array.from(accountIdsWithoutStorageBalancesSet).reduce((acc, accountId) => {
    if (acc[acc.length - 1].length >= 10) {
      acc.push([]);
    }

    acc[acc.length - 1].push(accountId);

    return acc;
  }, [[]]);

  await Promise.all(accountIdsInTens.map(async (ten) => {
    const depositAmount = nearAPI.utils.format.parseNearAmount('0.00125'); // account creation costs 0.00125 NEAR for storage

    const actions = ten.map((accountId) => nearAPI.transactions.functionCall(
      'storage_deposit',
      { account_id: accountId },
      '30000000000000',
      depositAmount,
    ));

    await retry(
      async () => {
        try {
          await senderAccount.signAndSendTransaction({
            receiverId: tokenContract.contractId,
            actions,
          });

          bar.increment(ten.length);

          const withStorageBalanceKey = `withStorageBalance_${tokenContract.contractId}`;

          const cache = (() => {
            try {
              const cacheString = fs.readFileSync(`${filename}.cache.json`, { encoding: 'utf-8' })
              return JSON.parse(cacheString);
            } catch {
              return {};
            }
          })();

          if (cache[withStorageBalanceKey]) {
            cache[withStorageBalanceKey].push(...ten);
          } else {
            cache[withStorageBalanceKey] = ten;
          }

          fs.writeFileSync(`${filename}.cache.json`, JSON.stringify(cache, null, 2));
        } catch (err) {
          if (
            err.message === 'Transaction has expired' ||
            err.message.includes(`GatewayTimeoutError`) ||
            err.message.includes(`Please try again`)
          ) {
            throw new Error('Try again');
          } else {
            console.log(`signAndSignTransaction error`);
            console.log(err);
            console.log(`Please try running the script with the same parameters again, continuing from the previous state.`);
            console.log(`If the error persists, contact developers from README.md.`);
            process.exit(1);
          }
        }
      },
      {
        retries: 10,
        minTimeout: 500,
        maxTimeout: 1500,
      }
    );
  }));

  console.log(`✔️ All needed FT storages were created.`);
}

async function createStreams(roketoContractName, lines, accountIdsWithoutStorageBalancesSet, options, ftMetadata, senderAccount, tokenContract, filename) {
  const streamCreatedKey = `streamCreated_from_${senderAccount.accountId}_in_${tokenContract.contractId}`;

  const streamCreatedCache = (() => {
    try {
      const cacheString = fs.readFileSync(`${filename}.cache.json`, { encoding: 'utf-8' })
      const cache = JSON.parse(cacheString);
      return cache[streamCreatedKey] ?? [];
    } catch {
      return [];
    }
  })();

  const remainingLinesToProcess = lines.filter(Boolean).filter((line) => !streamCreatedCache.includes(line));

  if (remainingLinesToProcess.length === 0) {
    console.log(`✔️ All streams were created in previous runs.`);
    return;
  }

  const bar = new cliProgress.MultiBar({
    stopOnComplete: true,
    forceRedraw: true,
    clearOnComplete: true,
  }, {
    ...cliProgress.Presets.shades_classic,
    format: 'Creating streams:' + cliProgress.Presets.shades_classic.format,
  }).create(remainingLinesToProcess.length, 0);

  let failedStreamsCount = 0;

  await Promise.all(lines.filter(Boolean).map(async (line) => {
    const [receiver, amount] = line.split(options.delimiter);

    const amountInYocto = new BigNumber(amount).multipliedBy(new BigNumber(10).exponentiatedBy(ftMetadata.decimals));

    const action = nearAPI.transactions.functionCall(
      'ft_transfer_call',
      {
        receiver_id: roketoContractName,
        amount: amountInYocto.toFixed(),
        memo: 'Roketo transfer',
        msg: JSON.stringify({
          Create: {
            request: {
              owner_id: senderAccount.accountId,
              receiver_id: receiver,
              balance: amountInYocto.toFixed(),
              tokens_per_sec: options.speed ?? amountInYocto.dividedToIntegerBy((options.endTimestamp - Date.now()) / 1000).toFixed(),
              ...options.cliffTimestamp && { cliff_period_sec: Math.floor((options.cliffTimestamp - Date.now()) / 1000) },
              ...options.locked && { is_locked: true },
              is_auto_start_enabled: true,
            },
          },
        }),
      },
      '100000000000000',
      1,
    );

    await retry(
      async () => {
        try {
          const finalExecutionOutcome = await senderAccount.signAndSendTransaction({
            receiverId: tokenContract.contractId,
            actions: [action],
          });

          const hasFailed = finalExecutionOutcome.receipts_outcome.some(
            (receipt) => receipt.outcome.status === 'Failure' || 'Failure' in receipt.outcome.status
          );

          bar.increment();

          if (hasFailed) {
            failedStreamsCount += 1;
            return;
          }

          const cache = (() => {
            try {
              const cacheString = fs.readFileSync(`${filename}.cache.json`, { encoding: 'utf-8' })
              return JSON.parse(cacheString);
            } catch {
              return {};
            }
          })();

          if (cache[streamCreatedKey]) {
            cache[streamCreatedKey].push(line);
          } else {
            cache[streamCreatedKey] = [line];
          }

          fs.writeFileSync(`${filename}.cache.json`, JSON.stringify(cache, null, 2));
        } catch (err) {
          if (
            err.message === 'Transaction has expired' ||
            err.message.includes(`GatewayTimeoutError`) ||
            err.message.includes(`Please try again`)
          ) {
            throw new Error('Try again');
          } else {
            console.log(`signAndSignTransaction error`);
            console.log(err);
            console.log(`Please try running the script with the same parameters again, continuing from the previous state.`);
            console.log(`If the error persists, contact developers from README.md.`);
            process.exit(1);
          }
        }
      },
      {
        retries: 10,
        minTimeout: 500,
        maxTimeout: 1500,
      }
    );
  }));

  if (failedStreamsCount > 0) {
    console.log(`The script failed to create ${failedStreamsCount}/${remainingLinesToProcess.length} streams.`);
    console.log(`Please try running the script with the same parameters again, continuing from the previous state.`);
    console.log(`If the error persists, contact developers from README.md.`);
  } else {
    console.log(`✔️ All streams were created.`);
  }
}

const main = async () => {
  checkCLIParams(options);

  const { roketoContractName, nearConfig } = getConfig(options.network);

  const near = await getNearInstance(nearConfig);

  const senderAccount = await near.account(options.senderAccountId);

  await checkSenderAccess(senderAccount);

  const lines = fs.readFileSync(options.csv.filename, { encoding: 'utf-8' }).split('\n');

  checkCSVCorrectness(lines, options.csv.filename);

  checkReceiversCorrectness(lines, options.senderAccountId);

  await checkReceiversExistence(lines, options.csv.filename, near);

  const tokenContract = new nearAPI.Contract(senderAccount, options.tokenAccountId, {
    viewMethods: ['ft_balance_of', 'ft_metadata', 'storage_balance_of'],
    changeMethods: ['ft_transfer_call', 'storage_deposit', 'near_deposit'],
  });

  const accountIdsWithoutStorageBalancesSet =
    await getAccountIdsWithoutStorageBalancesSet(options.senderAccountId, lines, options.csv.filename, tokenContract);

  await checkIfEnoughNEARs(senderAccount, accountIdsWithoutStorageBalancesSet);

  const ftMetadata = await tokenContract.ft_metadata();

  await checkIfEnoughFTs(tokenContract, ftMetadata, senderAccount, lines);

  printSummary(lines, options, ftMetadata);

  if (options.dryRun) {
    console.log('[dryRun] option specified, exiting...');
    process.exit(0);
  }

  await createStorageDeposits(accountIdsWithoutStorageBalancesSet, senderAccount, tokenContract, options.csv.filename);

  await createStreams(roketoContractName, lines, accountIdsWithoutStorageBalancesSet, options, ftMetadata, senderAccount, tokenContract, options.csv.filename);
};

main();
