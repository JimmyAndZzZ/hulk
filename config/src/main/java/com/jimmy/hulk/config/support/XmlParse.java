package com.jimmy.hulk.config.support;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jimmy.hulk.authority.base.AuthenticationManager;
import com.jimmy.hulk.authority.datasource.DatasourceCenter;
import com.jimmy.hulk.common.core.Database;
import com.jimmy.hulk.common.enums.DatasourceEnum;
import com.jimmy.hulk.common.enums.ModuleEnum;
import com.jimmy.hulk.common.enums.RoleEnum;
import com.jimmy.hulk.common.exception.HulkException;
import com.jimmy.hulk.config.properties.*;
import com.jimmy.hulk.data.config.DataSourceProperty;
import lombok.extern.slf4j.Slf4j;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.dom4j.tree.DefaultCDATA;
import org.dom4j.tree.DefaultText;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class XmlParse {

    private static final String CONFIG_XML_PATH = "booster.xml";

    @Autowired
    private TableConfig tableConfig;

    @Autowired
    private DatabaseConfig databaseConfig;

    @Autowired
    private DatasourceCenter datasourceCenter;

    @Autowired
    private AuthenticationManager authenticationManager;

    @Autowired
    private SystemVariableContext systemVariableContext;

    public void parse() {
        Map<String, DatasourceConfigProperty> propertiesMap = Maps.newHashMap();

        try (InputStream inputStream = XmlParse.class.getClassLoader().getResourceAsStream(CONFIG_XML_PATH)) {
            // 创建saxReader对象
            SAXReader reader = new SAXReader();
            // 通过read方法读取一个文件 转换成Document对象
            Document document = reader.read(inputStream);
            //获取根节点元素对象
            Element node = document.getRootElement();

            Element datasourceList = node.element("datasourceList");
            if (datasourceList == null) {
                throw new HulkException("数据源未配置", ModuleEnum.CONFIG);
            }

            List<Element> nodes = datasourceList.elements("datasource");
            if (CollUtil.isEmpty(nodes)) {
                throw new HulkException("数据源未配置", ModuleEnum.CONFIG);
            }
            //解析变量
            this.variableParse(node.element("system"));
            //数据源解析
            for (Element element : nodes) {
                String type = this.getAttributeValue(element, "type", true);

                DatasourceEnum byMessage = DatasourceEnum.getByMessage(type);
                if (byMessage == null) {
                    throw new HulkException("数据源类型为空", ModuleEnum.CONFIG);
                }

                String name = this.getContent(element, "name", true);

                DatasourceConfigProperty datasourceConfigProperty = new DatasourceConfigProperty();
                datasourceConfigProperty.setName(name);
                datasourceConfigProperty.setPassword(this.getContent(element, "password", false));
                datasourceConfigProperty.setSchema(this.getContent(element, "schema", false));
                datasourceConfigProperty.setClusterName(this.getContent(element, "clusterName", false));
                datasourceConfigProperty.setUrl(this.getContent(element, "url", true));
                datasourceConfigProperty.setUsername(this.getContent(element, "username", false));
                datasourceConfigProperty.setDs(byMessage);
                propertiesMap.put(name, datasourceConfigProperty);
            }
            //实例解析
            Element exampleElement = node.element("examples");
            if (datasourceList == null) {
                throw new HulkException("实例未配置", ModuleEnum.CONFIG);
            }

            List<Element> examples = exampleElement.elements("example");
            if (CollUtil.isEmpty(examples)) {
                throw new HulkException("实例未配置", ModuleEnum.CONFIG);
            }

            for (Element example : examples) {
                this.exampleParse(example, propertiesMap);
            }
            //解析分库分表信息
            Element partitions = node.element("partitions");
            if (partitions != null) {
                this.partitionParse(partitions);
            }
            //解析用户和权限信息
            Element usersElement = node.element("users");
            if (usersElement == null) {
                throw new HulkException("未配置用户名密码", ModuleEnum.CONFIG);
            }

            List<Element> users = usersElement.elements("user");
            if (CollUtil.isEmpty(users)) {
                throw new HulkException("未配置用户名密码", ModuleEnum.CONFIG);
            }

            for (Element user : users) {
                this.userParse(user, propertiesMap);
            }
        } catch (Exception e) {
            log.error("解析失败", e);
            throw new HulkException(e.getMessage(), ModuleEnum.CONFIG);
        }
    }

    /**
     * 系统配置变量解析
     *
     * @param systemElement
     */
    private void variableParse(Element systemElement) {
        if (systemElement == null) {
            return;
        }

        List<Element> property = systemElement.elements("property");
        if (CollUtil.isNotEmpty(property)) {
            for (Element element : property) {
                String name = this.getAttributeValue(element, "name", false);
                if (StrUtil.isNotBlank(name)) {
                    String value = this.get(element);
                    if (StrUtil.isEmpty(value)) {
                        throw new HulkException(name + "未配置具体值", ModuleEnum.CONFIG);
                    }

                    if (name.equalsIgnoreCase("pageSize")) {
                        systemVariableContext.setPageSize(Integer.valueOf(value));
                    }

                    if (name.equalsIgnoreCase("fileStorePath")) {
                        systemVariableContext.setFileStorePath(value);
                    }

                    if (name.equalsIgnoreCase("serializerType")) {
                        systemVariableContext.setSerializerType(value);
                    }

                    if (name.equalsIgnoreCase("defaultExpire")) {
                        systemVariableContext.setDefaultExpire(Integer.valueOf(value));
                    }

                    if (name.equalsIgnoreCase("port")) {
                        systemVariableContext.setPort(Integer.valueOf(value));
                    }

                    if (name.equalsIgnoreCase("transactionTimeout")) {
                        systemVariableContext.setTransactionTimeout(Integer.valueOf(value));
                    }
                }
            }
        }
    }

    /**
     * 用户解析
     *
     * @param element
     */
    private void userParse(Element element, Map<String, DatasourceConfigProperty> propertiesMap) {
        String username = this.getContent(element, "username", true);
        String password = this.getContent(element, "password", true);
        String role = this.getAttributeValue(element, "role", false);

        UserConfigProperty userConfigProperty = new UserConfigProperty();
        userConfigProperty.setUsername(username);
        userConfigProperty.setPassword(password);
        userConfigProperty.setRole(StrUtil.isNotBlank(role) ? RoleEnum.valueOf(role) : RoleEnum.GUEST);
        authenticationManager.registerUser(userConfigProperty.buildUserDetail());

        List<Element> schemas = element.elements("schema");
        if (CollUtil.isNotEmpty(schemas)) {
            Set<String> processSchemas = Sets.newHashSet();

            for (Element schema : schemas) {
                String dsName = this.getAttributeValue(schema, "dsName", true);
                if (!processSchemas.add(dsName)) {
                    throw new HulkException(dsName + "重复配置", ModuleEnum.CONFIG);
                }

                this.schemaParse(userConfigProperty.getRole(), schema, userConfigProperty, propertiesMap);
            }
        }
    }

    /**
     * schema解析
     *
     * @param element
     * @param userConfigProperty
     */
    private void schemaParse(RoleEnum roleEnum, Element element, UserConfigProperty userConfigProperty, Map<String, DatasourceConfigProperty> propertiesMap) {
        String dsName = this.getAttributeValue(element, "dsName", true);
        boolean isAllAllow = Convert.toBool(this.getAttributeValue(element, "isAllAllow", false), true);

        if (!propertiesMap.containsKey(dsName)) {
            throw new HulkException(dsName + "数据源信息不存在", ModuleEnum.CONFIG);
        }

        SchemaConfigProperty schemaConfigProperty = new SchemaConfigProperty();
        schemaConfigProperty.setDsName(dsName);
        schemaConfigProperty.setIsAllAllow(roleEnum.equals(RoleEnum.GUEST) ? false : isAllAllow);

        List<Element> tables = element.elements("table");
        if (CollUtil.isEmpty(tables) && !isAllAllow && !roleEnum.equals(RoleEnum.GUEST)) {
            throw new HulkException(dsName + "需要配置表信息", ModuleEnum.CONFIG);
        }

        if (CollUtil.isNotEmpty(tables) && !roleEnum.equals(RoleEnum.GUEST)) {
            Set<String> processTables = Sets.newHashSet();

            for (Element table : tables) {
                String tableName = this.getAttributeValue(table, "table", true);
                if (!processTables.add(tableName)) {
                    throw new HulkException(tableName + "重复配置", ModuleEnum.CONFIG);
                }

                this.authenticationTableParse(roleEnum, table, schemaConfigProperty);
            }
        }

        authenticationManager.configSchema(userConfigProperty.getUsername(), schemaConfigProperty.buildAuthenticationSchema());
    }

    /**
     * 表授权配置
     *
     * @param element
     * @param schemaConfigProperty
     */
    private void authenticationTableParse(RoleEnum roleEnum, Element element, SchemaConfigProperty schemaConfigProperty) {
        String table = this.getAttributeValue(element, "table", true);
        String filterFields = this.getAttributeValue(element, "filterFields", false);
        String dmlAllowMethods = this.getAttributeValue(element, "dmlAllowMethods", false);

        AuthenticationTableConfigProperty authenticationTableConfigProperty = new AuthenticationTableConfigProperty();
        authenticationTableConfigProperty.setTable(table);

        if (StrUtil.isNotBlank(filterFields)) {
            authenticationTableConfigProperty.setFilterFields(StrUtil.split(filterFields, ","));
        }

        if (StrUtil.isNotBlank(dmlAllowMethods) && !roleEnum.equals(RoleEnum.GUEST)) {
            authenticationTableConfigProperty.setDmlAllowMethods(StrUtil.split(dmlAllowMethods.toUpperCase(), ","));
        }

        schemaConfigProperty.getAuthenticationTableConfigProperties().add(authenticationTableConfigProperty);
    }

    /**
     * 实例解析
     *
     * @param example
     * @param propertiesMap
     */
    private void exampleParse(Element example, Map<String, DatasourceConfigProperty> propertiesMap) {
        String read = this.getContent(example, "read", false);
        String write = this.getContent(example, "write", true);
        String name = this.getAttributeValue(example, "name", true);
        String title = this.getAttributeValue(example, "title", false);
        boolean isReadOnly = Convert.toBool(this.getAttributeValue(example, "isReadOnly", false), false);

        DatasourceConfigProperty writeDatasourceConfigProperty = propertiesMap.get(write);
        if (writeDatasourceConfigProperty == null) {
            throw new HulkException(write + "数据源不存在", ModuleEnum.CONFIG);
        }

        List<DataSourceProperty> readDataSourceProperties = Lists.newArrayList();
        if (StrUtil.isNotBlank(read)) {
            List<String> split = StrUtil.split(read, ",");
            for (String s : split) {
                DatasourceConfigProperty readDatasourceConfigProperty = propertiesMap.get(s);
                if (readDatasourceConfigProperty == null) {
                    continue;
                }

                readDataSourceProperties.add(readDatasourceConfigProperty.getDataSourceProperty());
            }
        }

        datasourceCenter.add(name, writeDatasourceConfigProperty.getDataSourceProperty(), readDataSourceProperties, isReadOnly);
        this.tableConfigParse(example, name);

        Database database = new Database();
        database.setTitle(StrUtil.emptyToDefault(title, name));
        database.setDsName(name);
        database.setDs(writeDatasourceConfigProperty.getDs());
        databaseConfig.add(database);
    }

    /**
     * 表配置
     *
     * @param example
     */
    private void tableConfigParse(Element example, String dsName) {
        List<Element> partitions = example.elements("table");
        if (CollUtil.isEmpty(partitions)) {
            return;
        }

        for (Element partition : partitions) {
            TableConfigProperty tableConfigProperty = new TableConfigProperty();
            tableConfigProperty.setDsName(dsName);
            tableConfigProperty.setTableName(this.getAttributeValue(partition, "tableName", true));
            tableConfigProperty.setPriKeyName(this.getAttributeValue(partition, "priKeyName", true));
            tableConfigProperty.setPriKeyStrategy(this.getAttributeValue(partition, "priKeyStrategy", true));
            tableConfigProperty.setIsNeedReturnKey(Convert.toBool(this.getAttributeValue(partition, "isNeedReturnKey", false), false));
            tableConfig.putTableConfig(dsName, tableConfigProperty.getTableName(), tableConfigProperty);
        }
    }

    /**
     * 分区解析
     *
     * @param root
     */
    private void partitionParse(Element root) {
        List<Element> partitions = root.elements("partition");
        if (CollUtil.isEmpty(partitions)) {
            return;
        }

        for (Element partition : partitions) {
            String type = this.getAttributeValue(partition, "type", true);

            DatasourceEnum byMessage = DatasourceEnum.getByMessage(type);
            if (byMessage == null) {
                throw new HulkException("数据源类型为空", ModuleEnum.CONFIG);
            }

            PartitionConfigProperty partitionConfigProperty = new PartitionConfigProperty();
            partitionConfigProperty.setPartitionColumn(this.getAttributeValue(partition, "partitionColumn", true));
            partitionConfigProperty.setMod(this.getAttributeValue(partition, "mod", false));
            partitionConfigProperty.setDsName(this.getAttributeValue(partition, "dsName", true));
            partitionConfigProperty.setPriKeyColumn(this.getAttributeValue(partition, "priKeyColumn", true));
            partitionConfigProperty.setTable(this.getAttributeValue(partition, "table", true));
            partitionConfigProperty.setIsReadOnly(Convert.toBool(this.getAttributeValue(partition, "isReadOnly", false), false));
            partitionConfigProperty.setDatasourceEnum(byMessage);

            List<Element> tables = partition.elements("table");
            if (CollUtil.isEmpty(tables)) {
                throw new HulkException("未配置表列表", ModuleEnum.CONFIG);
            }

            for (Element table : tables) {
                PartitionTableConfigProperty partitionTableConfigProperty = new PartitionTableConfigProperty();
                partitionTableConfigProperty.setPrefix(this.getAttributeValue(table, "prefix", false));
                partitionTableConfigProperty.setRange(this.getAttributeValue(table, "range", false));
                partitionTableConfigProperty.setDsName(this.getAttributeValue(table, "dsName", true));

                List<Element> names = table.elements("name");
                if (CollUtil.isNotEmpty(names)) {
                    for (Element name : names) {
                        partitionTableConfigProperty.getTables().add(this.get(name));
                    }
                }

                partitionConfigProperty.getTableConfigProperties().add(partitionTableConfigProperty);
            }

            tableConfig.putPartitionConfig(partitionConfigProperty.getDsName(), partitionConfigProperty.getTable(), partitionConfigProperty);
        }
    }

    /**
     * 获得属性值
     *
     * @param parentElement
     * @param name
     * @param isRequired
     * @return
     */
    private String getAttributeValue(Element parentElement, String name, boolean isRequired) {
        String content = parentElement.attributeValue(name);
        if (StrUtil.isEmpty(content) && isRequired) {
            throw new HulkException(name + "属性未填写", ModuleEnum.CONFIG);
        }

        return content;
    }

    /**
     * 获取节点数据
     *
     * @param parentElement
     * @param name
     * @param isRequired
     * @return
     */
    private String getContent(Element parentElement, String name, boolean isRequired) {
        Element element = parentElement.element(name);
        if (element == null) {
            if (isRequired) {
                throw new HulkException(name + "属性未填写", ModuleEnum.CONFIG);
            }

            return null;
        }

        String content = this.get(element);
        if (StrUtil.isEmpty(content) && isRequired) {
            throw new HulkException(name + "属性未填写", ModuleEnum.CONFIG);
        }

        return content;
    }

    /**
     * 获取值
     *
     * @param element
     * @return
     */
    private String get(Element element) {
        List content = element.content();

        if (CollUtil.isEmpty(content)) {
            return null;
        }

        for (Object o : content) {
            if (o instanceof DefaultCDATA) {
                DefaultCDATA defaultCDATA = (DefaultCDATA) o;
                return StrUtil.trim(defaultCDATA.getText());
            }

            if (o instanceof DefaultText) {
                DefaultText defaultText = (DefaultText) o;
                String trim = StrUtil.trim(defaultText.getText());
                if (StrUtil.isNotBlank(trim)) {
                    return trim;
                }
            }
        }

        return null;
    }
}
