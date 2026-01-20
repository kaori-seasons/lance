/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.lance.catalog.namespace;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Lance Namespace Adapter 集成测试。
 * 
 * 本测试类覆盖了 LanceNamespaceAdapter 对 Table API 的增删改查操作，
 * 包括表的创建、查询、更新和删除等完整生命周期管理。
 * 
 * 测试范围：
 * - 命名空间管理：创建、列出、检查、删除
 * - 表管理：创建、查询、检查、删除
 * - 元数据操作：获取命名空间和表的元数据
 * - 错误处理：重复创建、不存在资源等异常场景
 */
@DisplayName("Lance Namespace Adapter 集成测试")
class LanceNamespaceAdapterITCase {
    
    @TempDir
    Path tempDir;
    
    private LanceNamespaceAdapter adapter;
    private String warehousePath;
    
    /**
     * 测试前初始化
     */
    @BeforeEach
    void setUp() {
        warehousePath = tempDir.resolve("warehouse").toString();
        
        // 创建配置
        Map<String, String> properties = new HashMap<>();
        properties.put(LanceNamespaceConfig.KEY_IMPL, "dir");
        properties.put(LanceNamespaceConfig.KEY_ROOT, warehousePath);
        
        // 创建适配器实例
        adapter = LanceNamespaceAdapter.create(properties);
        adapter.init();
    }
    
    /**
     * 测试后清理
     */
    @AfterEach
    void tearDown() throws Exception {
        if (adapter != null) {
            adapter.close();
        }
    }
    
    // ==================== 命名空间管理测试 ====================
    
    /**
     * 测试命名空间创建（Create）
     */
    @Test
    @DisplayName("测试创建命名空间")
    void testCreateNamespace() {
        // 准备
        String namespaceName = "test_db";
        Map<String, String> properties = new HashMap<>();
        properties.put("description", "Test database");
        
        // 执行
        adapter.createNamespace(properties, namespaceName);
        
        // 验证
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        
        // 验证元数据
        Map<String, String> metadata = adapter.getNamespaceMetadata(namespaceName);
        assertThat(metadata).isNotNull();
    }
    
    /**
     * 测试创建嵌套命名空间
     */
    @Test
    @DisplayName("测试创建嵌套命名空间")
    void testCreateNestedNamespace() {
        // 准备
        String parentNamespace = "parent_db";
        String childNamespace = "child_db";
        
        // 执行
        adapter.createNamespace(new HashMap<>(), parentNamespace);
        adapter.createNamespace(new HashMap<>(), parentNamespace, childNamespace);
        
        // 验证
        assertThat(adapter.namespaceExists(parentNamespace)).isTrue();
        assertThat(adapter.namespaceExists(parentNamespace, childNamespace)).isTrue();
    }
    
    /**
     * 测试列出命名空间（Read）
     */
    @Test
    @DisplayName("测试列出所有顶级命名空间")
    void testListNamespaces() {
        // 准备
        adapter.createNamespace(new HashMap<>(), "db1");
        adapter.createNamespace(new HashMap<>(), "db2");
        adapter.createNamespace(new HashMap<>(), "db3");
        
        // 执行
        List<String> namespaces = adapter.listNamespaces();
        
        // 验证
        assertThat(namespaces).isNotNull();
        assertThat(namespaces).contains("db1", "db2", "db3");
        assertThat(namespaces.size()).isGreaterThanOrEqualTo(3);
    }
    
    /**
     * 测试列出子命名空间
     */
    @Test
    @DisplayName("测试列出子命名空间")
    void testListChildNamespaces() {
        // 准备
        String parent = "my_warehouse";
        adapter.createNamespace(new HashMap<>(), parent);
        adapter.createNamespace(new HashMap<>(), parent, "schema1");
        adapter.createNamespace(new HashMap<>(), parent, "schema2");
        
        // 执行
        List<String> childNamespaces = adapter.listNamespaces(parent);
        
        // 验证
        assertThat(childNamespaces).isNotNull();
        assertThat(childNamespaces).contains("schema1", "schema2");
    }
    
    /**
     * 测试检查命名空间是否存在（Read）
     */
    @Test
    @DisplayName("测试检查命名空间存在性")
    void testNamespaceExists() {
        // 准备
        String namespaceName = "existing_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // 执行和验证
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        assertThat(adapter.namespaceExists("non_existing_db")).isFalse();
    }
    
    /**
     * 测试删除命名空间（Delete）
     */
    @Test
    @DisplayName("测试删除命名空间")
    void testDropNamespace() {
        // 准备
        String namespaceName = "temp_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        
        // 执行
        adapter.dropNamespace(false, namespaceName);
        
        // 验证
        assertThat(adapter.namespaceExists(namespaceName)).isFalse();
    }
    
    /**
     * 测试删除命名空间（级联删除）
     */
    @Test
    @DisplayName("测试级联删除命名空间及其内容")
    void testDropNamespaceCascade() {
        // 准备
        String namespaceName = "cascade_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // 执行
        adapter.dropNamespace(true, namespaceName);
        
        // 验证
        assertThat(adapter.namespaceExists(namespaceName)).isFalse();
    }
    
    /**
     * 测试获取命名空间元数据（Read）
     */
    @Test
    @DisplayName("测试获取命名空间元数据")
    void testGetNamespaceMetadata() {
        // 准备
        String namespaceName = "metadata_db";
        Map<String, String> properties = new HashMap<>();
        properties.put("owner", "admin");
        properties.put("environment", "test");
        
        adapter.createNamespace(properties, namespaceName);
        
        // 执行
        Map<String, String> metadata = adapter.getNamespaceMetadata(namespaceName);
        
        // 验证
        assertThat(metadata).isNotNull();
        assertThat(metadata).containsKeys("owner", "environment");
    }
    
    // ==================== 表管理测试 ====================
    
    /**
     * 测试创建表（Create）
     */
    @Test
    @DisplayName("测试在命名空间中创建表")
    void testCreateTable() {
        // 准备
        String namespaceName = "my_db";
        String tableName = "my_table";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format", "lance");
        
        // 执行
        adapter.createEmptyTable(tableLocation, tableProperties, namespaceName, tableName);
        
        // 验证
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
    }
    
    /**
     * 测试创建多个表
     */
    @Test
    @DisplayName("测试在同一命名空间中创建多个表")
    void testCreateMultipleTables() {
        // 准备
        String namespaceName = "test_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String[] tableNames = {"users", "products", "orders", "analytics"};
        
        // 执行
        for (String tableName : tableNames) {
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        // 验证
        List<String> tables = adapter.listTables(namespaceName);
        assertThat(tables).isNotNull();
        assertThat(tables).contains(tableNames);
    }
    
    /**
     * 测试列出表（Read）
     */
    @Test
    @DisplayName("测试列出命名空间中的所有表")
    void testListTables() {
        // 准备
        String namespaceName = "query_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        adapter.createEmptyTable(
            warehousePath + "/" + namespaceName + "/table1",
            new HashMap<>(),
            namespaceName, "table1"
        );
        adapter.createEmptyTable(
            warehousePath + "/" + namespaceName + "/table2",
            new HashMap<>(),
            namespaceName, "table2"
        );
        
        // 执行
        List<String> tables = adapter.listTables(namespaceName);
        
        // 验证
        assertThat(tables).isNotNull();
        assertThat(tables).contains("table1", "table2");
        assertThat(tables.size()).isGreaterThanOrEqualTo(2);
    }
    
    /**
     * 测试检查表是否存在（Read）
     */
    @Test
    @DisplayName("测试检查表存在性")
    void testTableExists() {
        // 准备
        String namespaceName = "check_db";
        String tableName = "check_table";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        
        // 执行和验证
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
        assertThat(adapter.tableExists(namespaceName, "non_existing_table")).isFalse();
    }
    
    /**
     * 测试获取表元数据（Read）
     */
    @Test
    @DisplayName("测试获取表元数据")
    void testGetTableMetadata() {
        // 准备
        String namespaceName = "metadata_db";
        String tableName = "metadata_table";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format", "lance");
        tableProperties.put("index", "ivf");
        
        adapter.createEmptyTable(tableLocation, tableProperties, namespaceName, tableName);
        
        // 执行
        AbstractLanceNamespaceAdapter.TableMetadata metadata = 
            adapter.getTableMetadata(namespaceName, tableName);
        
        // 验证
        assertThat(metadata).isNotNull();
        assertThat(metadata.getLocation()).isNotNull();
        assertThat(metadata.getStorageOptions()).isNotNull();
    }
    
    /**
     * 测试删除表（Delete）
     */
    @Test
    @DisplayName("测试删除表")
    void testDropTable() {
        // 准备
        String namespaceName = "drop_db";
        String tableName = "drop_table";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
        
        // 执行
        adapter.dropTable(namespaceName, tableName);
        
        // 验证
        assertThat(adapter.tableExists(namespaceName, tableName)).isFalse();
    }
    
    /**
     * 测试删除多个表
     */
    @Test
    @DisplayName("测试删除命名空间中的多个表")
    void testDropMultipleTables() {
        // 准备
        String namespaceName = "cleanup_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String[] tableNames = {"temp1", "temp2", "temp3"};
        for (String tableName : tableNames) {
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        // 验证创建成功
        for (String tableName : tableNames) {
            assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
        }
        
        // 执行 - 删除所有表
        for (String tableName : tableNames) {
            adapter.dropTable(namespaceName, tableName);
        }
        
        // 验证 - 所有表都被删除
        for (String tableName : tableNames) {
            assertThat(adapter.tableExists(namespaceName, tableName)).isFalse();
        }
    }
    
    // ==================== 综合场景测试 ====================
    
    /**
     * 测试完整的 CRUD 生命周期
     */
    @Test
    @DisplayName("测试完整的表 CRUD 生命周期")
    void testCompleteTableCrudLifecycle() {
        // 1. Create - 创建命名空间
        String namespaceName = "complete_db";
        Map<String, String> dbProps = new HashMap<>();
        dbProps.put("owner", "admin");
        adapter.createNamespace(dbProps, namespaceName);
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        
        // 2. Create - 创建表
        String tableName = "complete_table";
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        Map<String, String> tableProps = new HashMap<>();
        tableProps.put("format", "lance");
        adapter.createEmptyTable(tableLocation, tableProps, namespaceName, tableName);
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
        
        // 3. Read - 列出表
        List<String> tables = adapter.listTables(namespaceName);
        assertThat(tables).contains(tableName);
        
        // 4. Read - 获取表元数据
        AbstractLanceNamespaceAdapter.TableMetadata metadata = 
            adapter.getTableMetadata(namespaceName, tableName);
        assertThat(metadata.getLocation()).contains(tableName);
        
        // 5. Delete - 删除表
        adapter.dropTable(namespaceName, tableName);
        assertThat(adapter.tableExists(namespaceName, tableName)).isFalse();
        
        // 6. Delete - 删除命名空间
        adapter.dropNamespace(true, namespaceName);
        assertThat(adapter.namespaceExists(namespaceName)).isFalse();
    }
    
    /**
     * 测试多命名空间独立性
     */
    @Test
    @DisplayName("测试多个命名空间的独立性")
    void testMultipleNamespaceIndependence() {
        // 准备 - 创建多个独立的命名空间
        String db1 = "database1";
        String db2 = "database2";
        String tableName = "test_table";
        
        adapter.createNamespace(new HashMap<>(), db1);
        adapter.createNamespace(new HashMap<>(), db2);
        
        // 在 db1 中创建表
        adapter.createEmptyTable(
            warehousePath + "/" + db1 + "/" + tableName,
            new HashMap<>(),
            db1, tableName
        );
        
        // 在 db2 中创建相同名称的表
        adapter.createEmptyTable(
            warehousePath + "/" + db2 + "/" + tableName,
            new HashMap<>(),
            db2, tableName
        );
        
        // 验证 - 两个表独立存在
        assertThat(adapter.tableExists(db1, tableName)).isTrue();
        assertThat(adapter.tableExists(db2, tableName)).isTrue();
        
        // 验证 - 删除 db1 中的表不影响 db2
        adapter.dropTable(db1, tableName);
        assertThat(adapter.tableExists(db1, tableName)).isFalse();
        assertThat(adapter.tableExists(db2, tableName)).isTrue();
    }
    
    /**
     * 测试表名和命名空间名的特殊字符支持
     */
    @Test
    @DisplayName("测试支持下划线和数字的命名")
    void testSpecialCharacterNaming() {
        // 准备
        String namespaceName = "test_db_123";
        String tableName = "data_table_v2_001";
        
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
        adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        
        // 验证
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        assertThat(adapter.tableExists(namespaceName, tableName)).isTrue();
    }
    
    /**
     * 测试表数据量和性能
     */
    @Test
    @DisplayName("测试在单个命名空间中创建大量表")
    void testCreateManyTables() {
        // 准备
        String namespaceName = "scale_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        int tableCount = 50;
        
        // 执行 - 创建多个表
        for (int i = 0; i < tableCount; i++) {
            String tableName = "table_" + String.format("%03d", i);
            String tableLocation = warehousePath + "/" + namespaceName + "/" + tableName;
            adapter.createEmptyTable(tableLocation, new HashMap<>(), namespaceName, tableName);
        }
        
        // 验证
        List<String> tables = adapter.listTables(namespaceName);
        assertThat(tables).isNotNull();
        assertThat(tables.size()).isGreaterThanOrEqualTo(tableCount);
    }
    
    /**
     * 测试异常场景 - 创建已存在的命名空间
     */
    @Test
    @DisplayName("测试创建已存在的命名空间抛出异常")
    void testCreateExistingNamespaceThrowsException() {
        // 准备
        String namespaceName = "existing_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // 执行和验证 - 预期抛出异常
        assertThatThrownBy(() -> 
            adapter.createNamespace(new HashMap<>(), namespaceName)
        ).isNotNull();
    }
    
    /**
     * 测试异常场景 - 删除不存在的命名空间
     */
    @Test
    @DisplayName("测试删除不存在的命名空间抛出异常")
    void testDropNonExistingNamespaceThrowsException() {
        // 执行和验证 - 预期抛出异常
        assertThatThrownBy(() -> 
            adapter.dropNamespace(false, "non_existing_db")
        ).isNotNull();
    }
    
    /**
     * 测试异常场景 - 删除不存在的表
     */
    @Test
    @DisplayName("测试删除不存在的表抛出异常")
    void testDropNonExistingTableThrowsException() {
        // 准备
        String namespaceName = "error_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // 执行和验证 - 预期抛出异常
        assertThatThrownBy(() -> 
            adapter.dropTable(namespaceName, "non_existing_table")
        ).isNotNull();
    }
    
    /**
     * 测试资源清理和关闭
     */
    @Test
    @DisplayName("测试适配器正确关闭和资源释放")
    void testAdapterCloseAndResourceCleanup() throws Exception {
        // 准备
        String namespaceName = "cleanup_db";
        adapter.createNamespace(new HashMap<>(), namespaceName);
        
        // 验证操作正常
        assertThat(adapter.namespaceExists(namespaceName)).isTrue();
        
        // 执行 - 关闭适配器
        adapter.close();
        
        // 验证 - 创建新适配器时资源已正确释放
        Map<String, String> properties = new HashMap<>();
        properties.put(LanceNamespaceConfig.KEY_IMPL, "dir");
        properties.put(LanceNamespaceConfig.KEY_ROOT, warehousePath);
        
        LanceNamespaceAdapter newAdapter = LanceNamespaceAdapter.create(properties);
        newAdapter.init();
        
        try {
            // 验证之前的操作被保留
            assertThat(newAdapter.namespaceExists(namespaceName)).isTrue();
        } finally {
            newAdapter.close();
        }
    }
}
