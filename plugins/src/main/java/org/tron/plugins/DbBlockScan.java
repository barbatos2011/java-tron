package org.tron.plugins;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.rocksdb.RocksDBException;
import org.tron.core.capsule.TransactionCapsule;
import org.tron.core.exception.ContractValidateException;
import org.tron.plugins.utils.ByteArray;
import org.tron.common.utils.Sha256Hash;
import org.tron.plugins.utils.db.DBInterface;
import org.tron.plugins.utils.db.DBIterator;
import org.tron.plugins.utils.db.DbTool;
import org.tron.protos.Protocol;
import org.tron.protos.contract.BalanceContract;
import org.tron.protos.contract.ShieldContract;
import picocli.CommandLine;
import org.tron.core.capsule.BlockCapsule.BlockId;


@Slf4j(topic = "block-scan")
@CommandLine.Command(name = "block-scan",
        description = "scan data from block.",
        exitCodeListHeading = "Exit Codes:%n",
        exitCodeList = {
                "0:Successful",
                "n:query failed,please check toolkit.log"})
public class DbBlockScan implements Callable<Integer> {

    @CommandLine.Spec
    CommandLine.Model.CommandSpec spec;
    @CommandLine.Parameters(index = "0",
            description = " db path for block")
    private Path db;

    @CommandLine.Option(names = {"-s", "--start"},
            defaultValue = "7588859",
            description = "start block. Default: ${DEFAULT-VALUE}")
    private long start;

    @CommandLine.Option(names = {"-h", "--help"}, help = true, description = "display a help message")
    private boolean help;

    private static final String DB = "block";

    private final AtomicLong cnt = new AtomicLong(0);
    private final AtomicLong scanTotal = new AtomicLong(0);


    @Override
    public Integer call() throws Exception {

        if (help) {
            spec.commandLine().usage(System.out);
            return 0;
        }

        if (!db.toFile().exists()) {
            logger.info(" {} does not exist.", db);
            spec.commandLine().getErr().println(spec.commandLine().getColorScheme()
                    .errorText(String.format("%s does not exist.", db)));
            return 404;
        }
        return query();
    }


    private int query() throws RocksDBException, IOException {
        try (DBInterface database = DbTool.getDB(this.db, DB);
             DBIterator iterator = database.iterator()) {
            long min = start;
            iterator.seek(ByteArray.fromLong(min));
            min = new BlockId(Sha256Hash.wrap(iterator.getKey())).getNum();
            iterator.seekToLast();
            long max = new BlockId(Sha256Hash.wrap(iterator.getKey())).getNum();
            long total = max - min + 1;
            spec.commandLine().getOut().format("scan block start from  %d to %d ", min, max).println();
            logger.info("scan block start from {} to {}", min, max);
            int maxQueryCount = 28800;
            int count = 0;
            HashMap<Protocol.Transaction.Contract.ContractType, Integer> typeMap = new HashMap<>();
            HashMap<Protocol.Transaction.Contract.ContractType, HashMap<ByteString, Integer>> addressMap = new HashMap<>();

            try (ProgressBar pb = new ProgressBar("block-scan", total)) {
                for (iterator.seek(ByteArray.fromLong(min)); iterator.hasNext(); iterator.next()) {
                    statisticByType(iterator.getKey(), iterator.getValue(), typeMap);
                    statisticBySender(iterator.getKey(), iterator.getValue(), addressMap);
                    pb.step();
                    pb.setExtraMessage("Reading...");
                    scanTotal.getAndIncrement();
                    count++;
                    if (count >= maxQueryCount) {
                        break;
                    }
                }
            }
            printCountMap(typeMap);
            printAddressMap(addressMap);
            spec.commandLine().getOut().format("total scan block size: %d", scanTotal.get()).println();
            logger.info("total scan block size: {}", scanTotal.get());
            spec.commandLine().getOut().format("illegal multi-sig  size: %d", cnt.get()).println();
            logger.info("illegal multi-sig size: {}", cnt.get());
        }
        return 0;
    }

    private void statisticByType(byte[] k, byte[] v, HashMap<Protocol.Transaction.Contract.ContractType, Integer> countMap) {
        try {
            Protocol.Block block = Protocol.Block.parseFrom(v);
            long num = block.getBlockHeader().getRawData().getNumber();
            List<Protocol.Transaction> list = block.getTransactionsList().stream()
                    .filter(trans -> trans.getSignatureCount() > 0).collect(Collectors.toList());
            list.forEach(transaction -> {
                //if (!check(transaction)) {

                Protocol.Transaction.Contract.ContractType type =
                        transaction.getRawData().getContract(0).getType();
                if (!countMap.containsKey(type)) {
                    countMap.put(type, 1);
                } else {
                    countMap.put(type, countMap.get(type) + 1);
                }

                //  Sha256Hash tid = DBUtils.getTransactionId(transaction);
                //  spec.commandLine().getOut().format("%d, %s ", num, tid).println();
                //  logger.info("{}, {} ", num, tid);
                //  cnt.getAndIncrement();
                //}
            });
        } catch (InvalidProtocolBufferException e) {
            logger.error("{},{}", k, v);
        }
    }

    private void statisticBySender(byte[] k, byte[] v, HashMap<Protocol.Transaction.Contract.ContractType, HashMap<ByteString, Integer>> countMap) {
        try {
            Protocol.Block block = Protocol.Block.parseFrom(v);
            List<Protocol.Transaction> list = block.getTransactionsList().stream().filter(trans -> trans.getRawData().getContractCount() > 0).collect(Collectors.toList());
            list.forEach(transaction -> {
                Protocol.Transaction.Contract.ContractType iType = null;
                ByteString secondKey = null;
                if (transaction.getRawData().getContract(0).getType() == Protocol.Transaction.Contract.ContractType.TransferContract) {
                    iType = Protocol.Transaction.Contract.ContractType.TransferContract;
                    Any contractParameter = transaction.getRawData().getContract(0).getParameter();
                    try {
                        BalanceContract.TransferContract transferContract = contractParameter.unpack(BalanceContract.TransferContract.class);
                        secondKey = transferContract.getOwnerAddress();
                    } catch (InvalidProtocolBufferException e) {
                        throw new RuntimeException(e);
                    }
                }
                if (iType != null) {
                    if (!countMap.containsKey(iType)) {
                        countMap.put(iType, new HashMap<>());
                    }
                    if (!countMap.get(iType).containsKey(secondKey)) {
                        countMap.get(iType).put(secondKey, 1);
                    } else {
                        countMap.get(iType).put(secondKey, countMap.get(iType).get(secondKey) + 1);
                    }
                }
            });
        } catch (InvalidProtocolBufferException e) {
            logger.error("statisticBySender error: ", e);
        }
    }

    private boolean check(Protocol.Transaction transaction) {
        return transaction.getSignatureList().stream().map(sign -> sign.substring(0, 32))
                .collect(Collectors.toSet()).size() == transaction.getSignatureCount();

    }

    private void printCountMap(HashMap<Protocol.Transaction.Contract.ContractType, Integer> countMap) {
        logger.info("printCountMap");
        for (Map.Entry<Protocol.Transaction.Contract.ContractType, Integer> entry : countMap.entrySet()) {
            logger.info("key= {} and value= {}", entry.getKey(), entry.getValue());
        }
    }

    private void printAddressMap(HashMap<Protocol.Transaction.Contract.ContractType, HashMap<ByteString, Integer>> countMap) {
        logger.info("printAddressMap");
        for (Map.Entry<Protocol.Transaction.Contract.ContractType, HashMap<ByteString, Integer>> entry : countMap.entrySet()) {
            logger.info("ContractType: {}", entry.getKey());
            for (Map.Entry<ByteString, Integer> entry1: entry.getValue().entrySet()) {
                logger.info("address= {} and value= {}", entry1.getKey(), entry1.getValue());
            }
        }
    }
}
