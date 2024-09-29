package org.tron.program;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.beust.jcommander.JCommander;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.*;

import com.google.common.primitives.Longs;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.tron.common.application.Application;
import org.tron.common.application.ApplicationFactory;
import org.tron.common.application.TronApplicationContext;
import org.tron.common.parameter.CommonParameter;
import org.tron.common.prometheus.Metrics;
import org.tron.common.utils.ByteArray;
import org.tron.common.utils.Commons;
import org.tron.common.utils.Sha256Hash;
import org.tron.common.utils.StringUtil;
import org.tron.core.ChainBaseManager;
import org.tron.core.Constant;
import org.tron.core.capsule.BlockCapsule;
import org.tron.core.capsule.ContractCapsule;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.capsule.TransactionRetCapsule;
import org.tron.core.config.DefaultConfig;
import org.tron.core.config.args.Args;
import org.tron.core.db.common.iterator.DBIterator;
import org.tron.core.exception.BadItemException;
import org.tron.core.net.P2pEventHandlerImpl;
import org.tron.core.services.RpcApiService;
import org.tron.core.services.http.FullNodeHttpApiService;
import org.tron.core.services.interfaceJsonRpcOnPBFT.JsonRpcServiceOnPBFT;
import org.tron.core.services.interfaceJsonRpcOnSolidity.JsonRpcServiceOnSolidity;
import org.tron.core.services.interfaceOnPBFT.RpcApiServiceOnPBFT;
import org.tron.core.services.interfaceOnPBFT.http.PBFT.HttpApiOnPBFTService;
import org.tron.core.services.interfaceOnSolidity.RpcApiServiceOnSolidity;
import org.tron.core.services.interfaceOnSolidity.http.solidity.HttpApiOnSolidityService;
import org.tron.core.services.jsonrpc.FullNodeJsonRpcHttpService;
import org.tron.protos.Protocol;
import org.tron.protos.contract.SmartContractOuterClass;

@Slf4j(topic = "app")
public class FullNode {

    // 发射前
    private static byte[] TOKEN_PURCHASE_TOPIC =
            Hex.decode("63abb62535c21a5d221cf9c15994097b8880cc986d82faf80f57382b998dbae5");
    private static byte[] TOKEN_SOLD_TOPIC =
            Hex.decode("9387a595ac4be9038bbb9751abad8baa3dcf219dd9e19abb81552bd521fe3546");
    private static byte[] TRX_RECEIVED =
            Hex.decode("1bab02886c659969cbb004cc17dc19be19f193323a306e26c669bedb29c651f7");
    private static String PUMP_BUY_METHOD_1 = "1cc2c911";
    private static String PUMP_BUY_METHOD_2 = "2f70d762";
    private static String PUMP_SELL_METHOD = "d19aa2b9";
    private static byte[] SUNPUMP_LAUNCH = Hex.decode("41c22dd1b7bc7574e94563c8282f64b065bc07b2fa");
    private static BigDecimal TRX_DIVISOR = new BigDecimal("1000000");
    private static BigDecimal TOKEN_DIVISOR = new BigDecimal("1000000000000000000");

    // 发射后
    private static String SWAP_BUY_METHOD_1 = "fb3bdb41"; // swapETHForExactTokens
    private static String SWAP_BUY_METHOD_2 = "7ff36ab5";
    private static String SWAP_BUY_METHOD_3 =
            "b6f9de95"; // swapExactETHForTokensSupportingFeeOnTransferTokens
    private static String SWAP_SELL_METHOD_1 = "18cbafe5";
    private static String SWAP_SELL_METHOD_2 = "4a25d94a"; // swapTokensForExactETH
    private static String SWAP_SELL_METHOD_3 =
            "791ac947"; // swapExactTokensForETHSupportingFeeOnTransferTokens
    private static String SWAP_METHOD = "38ed1739";
    private static byte[] SWAP_ROUTER = Hex.decode("41fF7155b5df8008fbF3834922B2D52430b27874f5");
    private static byte[] SUNSWAP_ROUTER = Hex.decode("41e95812D8D5B5412D2b9F3A4D5a87CA15C5c51F33");
    private static byte[] TRANSFER_TOPIC =
            Hex.decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
    private static byte[] SWAP_TOPIC =
            Hex.decode("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822");
    private static byte[] WTRX_HEX = Hex.decode("891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18");
    private static String WTRX = "891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
    private static String WTRX41 = "41891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
    private static String USDT = "a614f803B6FD780986A42c78Ec9c7f77e6DeD13C".toLowerCase();

    private static String OWNER_ADDRESS = "TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7";
    private static String CONTRACT_ADDRESS = "TZFs5ch1R1C4mmjwrrmZqeqbUgGpxY1yWB";

    private static String get41Addr(String hexAddr) {
        if (!hexAddr.startsWith("41")) {
            return "41" + hexAddr;
        }
        return hexAddr;
    }

    public static void load(String path) {
        try {
            File file = new File(path);
            if (!file.exists() || !file.isFile() || !file.canRead()) {
                return;
            }
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(file);
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * Start the FullNode.
     */
    public static void main(String[] args) {
        logger.info("Full node running.");
        Args.setParam(args, Constant.TESTNET_CONF);
        CommonParameter parameter = Args.getInstance();

        load(parameter.getLogbackPath());

        if (parameter.isHelp()) {
            JCommander jCommander = JCommander.newBuilder().addObject(Args.PARAMETER).build();
            jCommander.parse(args);
            Args.printHelp(jCommander);
            return;
        }

        if (Args.getInstance().isDebug()) {
            logger.info("in debug mode, it won't check energy time");
        } else {
            logger.info("not in debug mode, it will check energy time");
        }

        // init metrics first
        Metrics.init();

        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.setAllowCircularReferences(false);
        TronApplicationContext context =
                new TronApplicationContext(beanFactory);
        context.register(DefaultConfig.class);
        context.refresh();
        Application appT = ApplicationFactory.create(context);
        context.registerShutdownHook();

        // grpc api server
        RpcApiService rpcApiService = context.getBean(RpcApiService.class);
        appT.addService(rpcApiService);

        // http api server
        FullNodeHttpApiService httpApiService = context.getBean(FullNodeHttpApiService.class);
        if (CommonParameter.getInstance().fullNodeHttpEnable) {
            appT.addService(httpApiService);
        }

        // JSON-RPC http server
        if (CommonParameter.getInstance().jsonRpcHttpFullNodeEnable) {
            FullNodeJsonRpcHttpService jsonRpcHttpService =
                    context.getBean(FullNodeJsonRpcHttpService.class);
            appT.addService(jsonRpcHttpService);
        }

        // full node and solidity node fuse together
        // provide solidity rpc and http server on the full node.
        RpcApiServiceOnSolidity rpcApiServiceOnSolidity = context
                .getBean(RpcApiServiceOnSolidity.class);
        appT.addService(rpcApiServiceOnSolidity);
        HttpApiOnSolidityService httpApiOnSolidityService = context
                .getBean(HttpApiOnSolidityService.class);
        if (CommonParameter.getInstance().solidityNodeHttpEnable) {
            appT.addService(httpApiOnSolidityService);
        }

        // JSON-RPC on solidity
        if (CommonParameter.getInstance().jsonRpcHttpSolidityNodeEnable) {
            JsonRpcServiceOnSolidity jsonRpcServiceOnSolidity = context
                    .getBean(JsonRpcServiceOnSolidity.class);
            appT.addService(jsonRpcServiceOnSolidity);
        }

        // PBFT API (HTTP and GRPC)
        RpcApiServiceOnPBFT rpcApiServiceOnPBFT = context
                .getBean(RpcApiServiceOnPBFT.class);
        appT.addService(rpcApiServiceOnPBFT);
        HttpApiOnPBFTService httpApiOnPBFTService = context
                .getBean(HttpApiOnPBFTService.class);
        appT.addService(httpApiOnPBFTService);

        // JSON-RPC on PBFT
        if (CommonParameter.getInstance().jsonRpcHttpPBFTNodeEnable) {
            JsonRpcServiceOnPBFT jsonRpcServiceOnPBFT = context.getBean(JsonRpcServiceOnPBFT.class);
            appT.addService(jsonRpcServiceOnPBFT);
        }
    /*
    appT.startup();
    appT.blockUntilShutdown();
     */

        try {
//      blockTransStat(startBlock, endBlock, ownerAddr, contractAddr);

            System.out.println("-- job start --");
//           filterTransactionAndToken();
            findAttackTransactions();
            System.out.println("-- job end --");
        } catch (Exception e) {
            logger.error("blockTransStat=>Exception:{}", e);
            System.out.println(e.getMessage());
            System.out.println("-- job end --");
        }
        logger.info("blockTransStat=>end");
    }

    private static void filterTransactionAndToken() throws BadItemException, InvalidProtocolBufferException {
        long startBlock = 64517095L;
//        long endBlock = 65352295L;
        long endBlock = 64805095L;
//        long startBlock = 65626700L;
//        long endBlock = 65627662L;
        final String OWNER_ADDRESS = "TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7";
        final String CONTRACT_ADDRESS = "TZFs5ch1R1C4mmjwrrmZqeqbUgGpxY1yWB";

        DBIterator blockIterator =
                (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
        blockIterator.seek(ByteArray.fromLong(startBlock));

        while (blockIterator.hasNext()) {
            Map.Entry<byte[], byte[]> blockEntry = blockIterator.next();
            BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());

            if (blockCapsule.getNum() > endBlock) {
                break;
            }

            for (TransactionCapsule tx : blockCapsule.getTransactions()) {
                Protocol.Transaction transaction = tx.getInstance();
                Protocol.Transaction.Contract.ContractType type =
                        transaction.getRawData().getContract(0).getType();
                if (type == Protocol.Transaction.Contract.ContractType.TriggerSmartContract) {
                    SmartContractOuterClass.TriggerSmartContract contract = ContractCapsule.getTriggerContractFromTransaction(transaction);
                    assert contract != null;
                    String ownerAddress = StringUtil.encode58Check(contract.getOwnerAddress().toByteArray());
                    String contractAddress = StringUtil.encode58Check(contract.getContractAddress().toByteArray());
                    if (OWNER_ADDRESS.equals(ownerAddress)) {
                        if (CONTRACT_ADDRESS.equals(contractAddress)) {
                            SmartContractOuterClass.TriggerSmartContract triggerSmartContract =
                                    tx.getInstance()
                                            .getRawData()
                                            .getContract(0)
                                            .getParameter()
                                            .unpack(SmartContractOuterClass.TriggerSmartContract.class);
                            String token = absToken(triggerSmartContract);
                            JSONObject obj = new JSONObject();
                            obj.put("tx", Hex.toHexString(tx.getTransactionId().getBytes()));
                            obj.put("block_num", blockCapsule.getNum());
                            if (StringUtils.isNotEmpty(token)) {
                                obj.put("token", token);
                            } else {
                                obj.put("token", "");
                            }

                            obj.put("res", transaction.getRet(0).getContractRet().name());
                            obj.put("sr",  StringUtil.encode58Check(blockCapsule.getWitnessAddress().toByteArray()));

                            System.out.println(obj.toJSONString());
                        }
                    }
                }
            }
        }
    }

    private static String absToken(SmartContractOuterClass.TriggerSmartContract triggerSmartContract) throws InvalidProtocolBufferException {
        String callData = Hex.toHexString(triggerSmartContract.getData().toByteArray());

        String token = null;
        if (callData.startsWith(SWAP_SELL_METHOD_1)) {
            if (callData.length() < 392) {
                token = get41Addr(callData.substring(8, 8 + 64).substring(24));
            } else {
                String token1 = callData.substring(392, 392 + 64).substring(24); // token1
                String token2 = callData.substring(456).substring(24); // token2 wtrx
                token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
            }
        } else if (callData.startsWith(SWAP_SELL_METHOD_2)) {
            String token1 = callData.substring(392, 392 + 64).substring(24); // token1
            String token2 =
                    callData.length() >= 456
                            ? callData.substring(456).substring(24)
                            : null; // token2 wtrx
            token =
                    (token1.equalsIgnoreCase(WTRX) && token2 != null)
                            ? get41Addr(token2)
                            : get41Addr(token1);
        } else if (callData.startsWith(SWAP_SELL_METHOD_3)) {
            token = get41Addr(callData.substring(392, 392 + 64)); // token
        } else if (callData.startsWith(SWAP_METHOD)) {
            //                String data1 = callData.substring(8, 8 + 64); // trx amount
            String token1 = callData.substring(392, 392 + 64).substring(24); // out token
            String token2 = callData.substring(456, 456 + 64).substring(24); // in token
            if (token1.equalsIgnoreCase(WTRX) || token1.equalsIgnoreCase(USDT)) {
                token = get41Addr(token2);
            } else {
                token = get41Addr(token1);
            }
        } else if (callData.startsWith(SWAP_BUY_METHOD_1)) {
            String token1 = callData.substring(392, 392 + 64).substring(24); // token1
            String token2 = callData.substring(328, 328 + 64).substring(24); // token2 wtrx
            token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
        } else if (callData.startsWith(SWAP_BUY_METHOD_2)) {
            String token1 = callData.substring(392, 392 + 64).substring(24); // token1
            String token2 = callData.substring(328, 328 + 64).substring(24); // token2 wtrx
            token = token1.equalsIgnoreCase(WTRX) ? get41Addr(token2) : get41Addr(token1);
        } else if (callData.startsWith(SWAP_BUY_METHOD_3)) {
            token = get41Addr(callData.substring(392, 392 + 64)); // token
        }
        return token;
    }

    private static String absSelector(SmartContractOuterClass.TriggerSmartContract triggerSmartContract) throws InvalidProtocolBufferException {
        String callData = Hex.toHexString(triggerSmartContract.getData().toByteArray());
        if (callData.startsWith(SWAP_SELL_METHOD_1)) {
            return "Sell";
        } else if (callData.startsWith(SWAP_SELL_METHOD_2)) {
            return "Sell";
        } else if (callData.startsWith(SWAP_SELL_METHOD_3)) {
            return "Sell";
        } else if (callData.startsWith(SWAP_METHOD)) {
            String token1 = callData.substring(392, 392 + 64).substring(24); // out token
            if (token1.equalsIgnoreCase(WTRX) || token1.equalsIgnoreCase(USDT)) {
                return "Buy";
            } else {
                return "Sell";
            }
        } else if (callData.startsWith(SWAP_BUY_METHOD_1)) {
            return "Buy";
        } else if (callData.startsWith(SWAP_BUY_METHOD_2)) {
            return "Buy";
        } else if (callData.startsWith(SWAP_BUY_METHOD_3)) {
            return "Buy";
        }
        return "";
    }

    private static final String JSON_FILE_PATH = "/data/yk/nodeMap.json";

    private static void findAttackTransactions() throws BadItemException, IOException {
        long startBlock = 64568170L;
        long endBlock = 65352295L;
        HashMap<String, HashSet<String>> blockTokenMap = new HashMap<>();

        // 使用 BufferedReader 和 FileReader 读取 JSON 文件内容
        try (BufferedReader reader = new BufferedReader(new FileReader(JSON_FILE_PATH))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 使用 fastjson 将字符串转换为 JSONObject
                JSONObject jsonObject = JSON.parseObject(line);
                String mKey = jsonObject.getString("block_num");
                if (!blockTokenMap.containsKey(mKey)) {
                    blockTokenMap.put(mKey, new HashSet<>());
                }
                blockTokenMap.get(mKey).add(jsonObject.getString("token"));
            }
        } catch (IOException e) {
            System.out.println(e.getMessage());
//        throw new RuntimeException(e);
        }

        System.out.println(blockTokenMap);

        DBIterator retIterator =
                (DBIterator) ChainBaseManager.getInstance().getTransactionRetStore().getDb().iterator();
        retIterator.seek(ByteArray.fromLong(startBlock));
        DBIterator blockIterator =
                (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
        blockIterator.seek(ByteArray.fromLong(startBlock));

        while (retIterator.hasNext() && blockIterator.hasNext()) {
            Map.Entry<byte[], byte[]> retEntry = retIterator.next();
            Map.Entry<byte[], byte[]> blockEntry = blockIterator.next();
            byte[] key = retEntry.getKey();
            long blockNum = Longs.fromByteArray(key);
            long blockStoreNum = Longs.fromByteArray(blockEntry.getKey());
            while (blockNum != blockStoreNum) {
                blockEntry = blockIterator.next();
                blockStoreNum = Longs.fromByteArray(blockEntry.getKey());
            }
            if (blockNum > endBlock) {
                break;
            }
            if (blockTokenMap.containsKey(String.valueOf(blockNum+1))) {
                System.out.println(blockNum);
                combineSingleAttackBlocks(blockTokenMap, blockNum, retEntry, blockEntry, retIterator, blockIterator);
//                return;
            }
        }
    }

    private static void combineSingleAttackBlocks(HashMap<String, HashSet<String>> blockTokenMap, long blockNum, Map.Entry<byte[], byte[]> retEntry, Map.Entry<byte[], byte[]> blockEntry, DBIterator retIterator, DBIterator blockIterator) throws BadItemException, IOException {
        JSONArray objAttacker = new JSONArray();
        JSONArray objNormal = new JSONArray();
        for (int i = 0; i < 3; i++) {
            System.out.println("-- job start0 --");

            if (i > 0) {
                if (retIterator.hasNext() && blockIterator.hasNext()) {
                    retEntry = retIterator.next();
                    blockEntry = blockIterator.next();
                    byte[] key = retEntry.getKey();
                    long blockNum1 = Longs.fromByteArray(key);
                    long blockStoreNum = Longs.fromByteArray(blockEntry.getKey());
                    if (blockStoreNum > blockNum + 2 || blockNum1 > blockNum + 2) {
                        break;
                    }
                    while (blockNum1 != blockStoreNum) {
                        blockEntry = blockIterator.next();
                        blockStoreNum = Longs.fromByteArray(blockEntry.getKey());
                    }
                } else {
                    break;
                }
            }

            byte[] value = retEntry.getValue();
            TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
            BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());
            Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
            for (TransactionCapsule tx : blockCapsule.getTransactions()) {
                txCallerMap.put(tx.getTransactionId().toString(), tx);
            }
            int txIndex = 0;
            for (Protocol.TransactionInfo transactionInfo :
                    transactionRetCapsule.getInstance().getTransactioninfoList()) {
                txIndex++;

                byte[] txId = transactionInfo.getId().toByteArray();
                TransactionCapsule tx = txCallerMap.get(Hex.toHexString(txId));

                Protocol.Transaction transaction = tx.getInstance();
                Protocol.Transaction.Contract.ContractType type =
                        transaction.getRawData().getContract(0).getType();
                if (type == Protocol.Transaction.Contract.ContractType.TriggerSmartContract) {
                    SmartContractOuterClass.TriggerSmartContract contract = ContractCapsule.getTriggerContractFromTransaction(transaction);
                    assert contract != null;
                    String ownerAddress = StringUtil.encode58Check(contract.getOwnerAddress().toByteArray());
                    String contractAddress = StringUtil.encode58Check(contract.getContractAddress().toByteArray());
                    if (CONTRACT_ADDRESS.equals(contractAddress)) {
                        SmartContractOuterClass.TriggerSmartContract triggerSmartContract =
                                tx.getInstance()
                                        .getRawData()
                                        .getContract(0)
                                        .getParameter()
                                        .unpack(SmartContractOuterClass.TriggerSmartContract.class);

                        String token = absToken(triggerSmartContract);
                        if ((blockTokenMap.containsKey(String.valueOf(blockNum+1))) && blockTokenMap.get(String.valueOf(blockNum+1)).contains(token)) {
                            JSONObject objTx = new JSONObject();
                            String selector = absSelector(triggerSmartContract);
                            objTx.put("selector", selector);

                            objTx.put("tx_hash", Hex.toHexString(tx.getTransactionId().getBytes()));
                            objTx.put("tx_index", txIndex);
                            objTx.put("tx_sender", ownerAddress);
                            objTx.put("tx_expiration", transaction.getRawData().getExpiration());
                            objTx.put("tx_timestamp", transaction.getRawData().getTimestamp());

                            objTx.put("ret", transaction.getRet(0).getContractRet().name());

                            long energyFee = transactionInfo.getReceipt().getNetFee();
                            long netFee = transactionInfo.getReceipt().getEnergyFee();
                            objTx.put("fee_net", netFee);
                            objTx.put("fee_energy", energyFee);

                            objTx.put("token", token);

                            objTx.put("block_num", blockCapsule.getNum());
                            objTx.put("block_time", blockCapsule.getTimeStamp());
                            objTx.put("block_sr", StringUtil.encode58Check(blockCapsule.getWitnessAddress().toByteArray()));

                            if (transaction.getRet(0).getContractRet() == Protocol.Transaction.Result.contractResult.SUCCESS) {
                                Pair<BigDecimal, BigDecimal> pair = absTokenAmountForSuccess(transactionInfo);
                                if (pair != null) {
                                    objTx.put("tx_trx_amount", pair.getLeft().longValue());
                                    objTx.put("tx_token_amount", pair.getRight().longValue());
                                } else {
                                    objTx.put("tx_trx_amount", 0);
                                    objTx.put("tx_token_amount", 0);
                                }
                            } else {
                                BigDecimal tokenAmount = absTokenAmountForFailed(triggerSmartContract);
                                if (selector.equals("Buy")) {
                                    objTx.put("tx_trx_amount", triggerSmartContract.getCallValue());
                                    objTx.put("tx_token_amount", tokenAmount.longValue());
                                } else {
                                    objTx.put("tx_trx_amount", 0);
                                    objTx.put("tx_token_amount", tokenAmount.longValue());
                                }
                            }

                            if (OWNER_ADDRESS.equals(ownerAddress)) {
                                // attacker
                                objAttacker.add(objTx);
                            } else {
                                objNormal.add(objTx);
                            }
                        }
                    }
                }
            }
        }

        JSONObject objItem = new JSONObject();
        objItem.put("attacker", objAttacker);
        objItem.put("victim", objNormal);
        System.out.println(objItem.toJSONString());
    }

    private static Pair<BigDecimal, BigDecimal> absTokenAmountForSuccess(Protocol.TransactionInfo transactionInfo) throws IOException {
        Map<String, String> pairToTokenMap = populateMap();

        for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {
            if (!Arrays.equals(log.getTopics(0).toByteArray(), SWAP_TOPIC)) {
                continue;
            }
            // Swap topic
            String logData = Hex.toHexString(log.getData().toByteArray());
            BigInteger amount0In = new BigInteger(logData.substring(0, 64), 16);
            BigInteger amount1In = new BigInteger(logData.substring(64, 128), 16);
            BigInteger amount0Out = new BigInteger(logData.substring(128, 192), 16);
            BigInteger amount1Out = new BigInteger(logData.substring(192, 256), 16);

            String pair = Hex.toHexString(log.getAddress().toByteArray());
            String token = pairToTokenMap.get(pair);
            boolean tokenNull = token == null;

            if (tokenNull) {
                for (Protocol.TransactionInfo.Log log2 : transactionInfo.getLogList()) {
                    if (Arrays.equals(TRANSFER_TOPIC, log2.getTopics(0).toByteArray())
                            && !Arrays.equals(log2.getAddress().toByteArray(), WTRX_HEX)) {
                        token = Hex.toHexString(log2.getAddress().toByteArray());
                        break;
                    }
                }
            }

            boolean smaller = smallerToWtrx(token, WTRX);
            boolean isBuy =
                    ((smaller && amount0Out.compareTo(BigInteger.ZERO) > 0)
                            || (!smaller && amount1Out.compareTo(BigInteger.ZERO) > 0));

            BigDecimal trxAmount;
            BigDecimal tokenAmount;
            if (isBuy) {
                if (smaller) {
                    trxAmount =
                            new BigDecimal(amount1In).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                    tokenAmount =
                            new BigDecimal(amount0Out).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
                } else {
                    trxAmount =
                            new BigDecimal(amount0In).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                    tokenAmount =
                            new BigDecimal(amount1Out).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
                }
            } else {
                if (smaller) {
                    trxAmount =
                            new BigDecimal(amount1Out).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                    tokenAmount =
                            new BigDecimal(amount0In).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
                } else {
                    trxAmount =
                            new BigDecimal(amount0Out).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN);
                    tokenAmount =
                            new BigDecimal(amount1In).divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN);
                }
            }
            return Pair.of(trxAmount, tokenAmount);
        }
        return null;
    }

    private static BigDecimal absTokenAmountForFailed(SmartContractOuterClass.TriggerSmartContract triggerSmartContract) throws IOException {
        String callData = Hex.toHexString(triggerSmartContract.getData().toByteArray());

        BigDecimal tokenAmount = BigDecimal.ZERO;
        if (callData.startsWith(SWAP_SELL_METHOD_1)) {
            if (callData.length() >= 392) {
                tokenAmount =
                        new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                                .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
            }
        } else if (callData.startsWith(SWAP_SELL_METHOD_2)) {
            tokenAmount =
                    new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                            .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
        } else if (callData.startsWith(SWAP_BUY_METHOD_1)) {
            tokenAmount =
                    new BigDecimal(new BigInteger(callData.substring(8, 8 + 64), 16))
                            .divide(TOKEN_DIVISOR, 18, RoundingMode.HALF_EVEN); // token 个数
        }
        return tokenAmount;
    }

    private static void blockTransStat(
            long startBlock,
            long endBlock,
            String ownerAddr,
            String contractAddr)
            throws Exception {
        logger.info("blockTransStat=>blockTransStat start");

        final String OWNER_ADDRESS = "TPsUGKAoXDSFz332ZYtTGdDHWzftLYWFj7";
        final String CONTRACT_ADDRESS = "TZFs5ch1R1C4mmjwrrmZqeqbUgGpxY1yWB";
        byte[] SWAP_TOPIC = Hex.decode("d78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822");


        byte[] TRANSFER_TOPIC =
                Hex.decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef");
        byte[] WTRX_HEX = Hex.decode("891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18");
        String WTRX = "891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
        String WTRX41 = "41891cdb91d149f23B1a45D9c5Ca78a88d0cB44C18";
        String USDT = "a614f803B6FD780986A42c78Ec9c7f77e6DeD13C".toLowerCase();
        BigDecimal TRX_DIVISOR = new BigDecimal("1000000");

        Map<String, String> pairToTokenMap = populateMap();

        DBIterator retIterator =
                (DBIterator) ChainBaseManager.getInstance().getTransactionRetStore().getDb().iterator();
        retIterator.seek(ByteArray.fromLong(startBlock));
        DBIterator blockIterator =
                (DBIterator) ChainBaseManager.getInstance().getBlockStore().getDb().iterator();
        blockIterator.seek(ByteArray.fromLong(startBlock));
        while (retIterator.hasNext() && blockIterator.hasNext()) {
            Map.Entry<byte[], byte[]> retEntry = retIterator.next();
            Map.Entry<byte[], byte[]> blockEntry = blockIterator.next();
            byte[] key = retEntry.getKey();
            long blockNum = Longs.fromByteArray(key);
            long blockStoreNum = Longs.fromByteArray(blockEntry.getKey());
            while (blockNum != blockStoreNum) {
                blockEntry = blockIterator.next();
                blockStoreNum = Longs.fromByteArray(blockEntry.getKey());
            }
            if (blockNum > endBlock) {
                logger.info("blockTransStat=>blockNum:{} > endBlock:{}", blockNum, endBlock);
                break;
            }

            long energyFee = 0L;
            long netFee = 0L;

            byte[] value = retEntry.getValue();
            TransactionRetCapsule transactionRetCapsule = new TransactionRetCapsule(value);
            BlockCapsule blockCapsule = new BlockCapsule(blockEntry.getValue());
            Map<String, TransactionCapsule> txCallerMap = new HashMap<>();
            for (TransactionCapsule tx : blockCapsule.getTransactions()) {
                txCallerMap.put(tx.getTransactionId().toString(), tx);
            }
            for (Protocol.TransactionInfo transactionInfo :
                    transactionRetCapsule.getInstance().getTransactioninfoList()) {
                byte[] txId = transactionInfo.getId().toByteArray();
                TransactionCapsule tx = txCallerMap.get(Hex.toHexString(txId));

                Protocol.Transaction transaction = tx.getInstance();
                Protocol.Transaction.Contract.ContractType type =
                        transaction.getRawData().getContract(0).getType();
                if (type == Protocol.Transaction.Contract.ContractType.TriggerSmartContract) {
                    SmartContractOuterClass.TriggerSmartContract contract = ContractCapsule.getTriggerContractFromTransaction(transaction);
                    String ownerAddress = StringUtil.encode58Check(contract.getOwnerAddress().toByteArray());
                    String contractAddress = StringUtil.encode58Check(contract.getContractAddress().toByteArray());
                    if (OWNER_ADDRESS.equals(ownerAddress) && CONTRACT_ADDRESS.equals(contractAddress)) {

                        SmartContractOuterClass.TriggerSmartContract triggerSmartContract =
                                tx.getInstance()
                                        .getRawData()
                                        .getContract(0)
                                        .getParameter()
                                        .unpack(SmartContractOuterClass.TriggerSmartContract.class);
                        String callData = Hex.toHexString(triggerSmartContract.getData().toByteArray());
                        BigDecimal trxTotalAmount = BigDecimal.ZERO;
                        for (Protocol.TransactionInfo.Log log : transactionInfo.getLogList()) {
                            if (!Arrays.equals(log.getTopics(0).toByteArray(), SWAP_TOPIC)) {
                                continue;
                            }
                            String logData = Hex.toHexString(log.getData().toByteArray());
                            BigInteger amount0In = new BigInteger(logData.substring(0, 64), 16);
                            BigInteger amount1In = new BigInteger(logData.substring(64, 128), 16);
                            BigInteger amount0Out = new BigInteger(logData.substring(128, 192), 16);
                            BigInteger amount1Out = new BigInteger(logData.substring(192, 256), 16);

                            String pair = Hex.toHexString(log.getAddress().toByteArray());
                            String token = pairToTokenMap.get(pair);
                            boolean tokenNull = token == null;
                            if (tokenNull) {
                                for (Protocol.TransactionInfo.Log log2 : transactionInfo.getLogList()) {
                                    if (Arrays.equals(TRANSFER_TOPIC, log2.getTopics(0).toByteArray())
                                            && !Arrays.equals(log2.getAddress().toByteArray(), WTRX_HEX)) {
                                        token = Hex.toHexString(log2.getAddress().toByteArray());
                                        break;
                                    }
                                }
                            }
                            boolean smaller = smallerToWtrx(token, WTRX);
                            boolean isBuy =
                                    ((smaller && amount0Out.compareTo(BigInteger.ZERO) > 0)
                                            || (!smaller && amount1Out.compareTo(BigInteger.ZERO) > 0));

                            if (!isBuy) {
                                if (smaller) {
                                    trxTotalAmount = trxTotalAmount.add(
                                            new BigDecimal(amount1Out).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN));
                                } else {
                                    trxTotalAmount = trxTotalAmount.add(
                                            new BigDecimal(amount0Out).divide(TRX_DIVISOR, 6, RoundingMode.HALF_EVEN));
                                }
                            }
                        }

                        netFee = transactionInfo.getReceipt().getNetFee();
                        energyFee = transactionInfo.getReceipt().getEnergyFee();

                        if (trxTotalAmount.compareTo(BigDecimal.ZERO) > 0) {
                            try {
                                logger.info("blockTransStat=> blockNum:{},headTime:{},transHash:{}" +
                                                ",transRet:{},contractJson:{},trxOutAmount:{},energyFee:{},netFee:{}",
                                        blockCapsule.getInstance().getBlockHeader().getRawData().getNumber(),
                                        blockCapsule.getInstance().getBlockHeader().getRawData().getTimestamp(),
                                        getTransactionId(transaction).toString(),
                                        transaction.getRet(0).getContractRet().name(),
                                        JsonFormat.printer().omittingInsignificantWhitespace().print(contract).toString(),
                                        trxTotalAmount,
                                        energyFee,
                                        netFee
                                );
                            } catch (Exception e) {
                                logger.error("blockTransStat=>{}", e);
                            }
                        } else {
                            logger.info("blockTransStat=>trxTotalAmount:{}", trxTotalAmount);
                        }
                    }
                }
            }
        }
        logger.info("blockTransStat=>blockTransStat end");
    }

    private static Map<String, String> populateMap() throws IOException {
        // token pair map
        File file = new File("test.txt");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line;
        Map<String, String> map = new HashMap<>();
        while ((line = reader.readLine()) != null) {
            String[] strings = line.split(" ");
            String token = strings[0];
            String pair = strings[1];
            long blockNum = Long.parseLong(strings[2]);
            long timestamp = Long.parseLong(strings[3]);

            map.put(
                    Hex.toHexString(Commons.decodeFromBase58Check(pair)).substring(2),
                    Hex.toHexString(Commons.decodeFromBase58Check(token)).substring(2));
        }
        return map;
    }

    private static boolean smallerToWtrx(String token, String wtrx) {
        final Map<String, Boolean> compareMap = new HashMap<>();
        return compareMap.computeIfAbsent(token, t -> token.compareTo(wtrx) < 0);
    }

    private static Sha256Hash getTransactionId(Protocol.Transaction transaction) {
        return Sha256Hash.of(true,
                transaction.getRawData().toByteArray());
    }

}
