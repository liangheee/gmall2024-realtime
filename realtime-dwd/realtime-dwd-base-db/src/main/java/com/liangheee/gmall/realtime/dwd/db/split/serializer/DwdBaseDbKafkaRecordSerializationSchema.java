package com.liangheee.gmall.realtime.dwd.db.split.serializer;

import com.alibaba.fastjson.JSONObject;
import com.liangheee.gmall.realtime.common.bean.TableProcessDwd;
import com.liangheee.gmall.realtime.common.utils.ColumnUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @author liangheee
 * * @date 2024/11/19
 */
public class DwdBaseDbKafkaRecordSerializationSchema implements KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>> {
    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<JSONObject, TableProcessDwd> element, KafkaSinkContext context, Long timestamp) {
        JSONObject jsonObj = element.f0;
        TableProcessDwd tableProcessDwd = element.f1;
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");
        String sinkTable = tableProcessDwd.getSinkTable();
        String sinkColumns = tableProcessDwd.getSinkColumns();
        ColumnUtil.deleteNotNeedColumns(dataJsonObj, sinkColumns);
        return new ProducerRecord<>(sinkTable, Bytes.toBytes(dataJsonObj.toJSONString()));
    }
}
