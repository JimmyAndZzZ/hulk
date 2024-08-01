package com.jimmy.hulk.data.datasource;

import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.data.actuator.Actuator;
import com.jimmy.hulk.data.actuator.ElasticsearchActuator;
import com.jimmy.hulk.data.core.Dump;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

import static com.jimmy.hulk.common.enums.DatasourceEnum.ELASTICSEARCH;

public class ElasticsearchDatasource extends BaseDatasource<RestHighLevelClient> {

    @Override
    public void close() throws IOException {

    }

    @Override
    public Actuator getActuator() {
        return new ElasticsearchActuator(this, dataSourceProperty);
    }

    @Override
    public RestHighLevelClient getDataSource() {
        String[] split = dataSourceProperty.getUrl().split(",");
        //创建HttpHost数组，其中存放es主机和端口的配置信息
        HttpHost[] httpHostArray = new HttpHost[split.length];
        for (int i = 0; i < split.length; i++) {
            String item = split[i];
            httpHostArray[i] = new HttpHost(item.split(":")[0], Integer.parseInt(item.split(":")[1]), "http");
        }

        return new RestHighLevelClient(RestClient.builder(httpHostArray));
    }

    @Override
    public RestHighLevelClient getDataSource(Long timeout) {
        return this.getDataSource();
    }

    @Override
    public boolean testConnect() {
        RestHighLevelClient restHighLevelClient = null;

        try {
            restHighLevelClient = this.getDataSource();
            restHighLevelClient.ping(RequestOptions.DEFAULT);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            try {
                if (restHighLevelClient != null) {
                    restHighLevelClient.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void dump(Dump dump) throws Exception {

    }

    @Override
    public RestHighLevelClient getDataSourceWithoutCache(Long timeout) {
        return this.getDataSource();
    }

    @Override
    public DatasourceEnum type() {
        return ELASTICSEARCH;
    }
}
