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

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateEmptyTableRequest;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DescribeTableRequest;
import org.lance.namespace.model.DescribeTableResponse;
import org.lance.namespace.model.DropNamespaceRequest;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.ListNamespacesRequest;
import org.lance.namespace.model.ListNamespacesResponse;
import org.lance.namespace.model.ListTablesRequest;
import org.lance.namespace.model.ListTablesResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Lance Namespace Adapter Implementation.
 * 
 * 直接调用 Lance Namespace SDK 的具体 API，实现数据库和表的管理功能。
 * 支持本地文件系统和 REST 两种后端实现。
 */
public class LanceNamespaceAdapter implements AbstractLanceNamespaceAdapter {
    
    private static final Logger LOG = LoggerFactory.getLogger(LanceNamespaceAdapter.class);
    
    private final BufferAllocator allocator;
    private final LanceNamespaceConfig config;
    private LanceNamespace namespace;
    
    public LanceNamespaceAdapter(BufferAllocator allocator, LanceNamespaceConfig config) {
        this.allocator = Objects.requireNonNull(allocator, "BufferAllocator cannot be null");
        this.config = Objects.requireNonNull(config, "LanceNamespaceConfig cannot be null");
    }
    
    /**
     * 工厂方法创建 Adapter 实例
     */
    public static LanceNamespaceAdapter create(Map<String, String> properties) {
        LanceNamespaceConfig config = LanceNamespaceConfig.from(properties);
        BufferAllocator allocator = new RootAllocator();
        return new LanceNamespaceAdapter(allocator, config);
    }
    
    /**
     * 初始化 Lance Namespace 连接
     * 直接调用 LanceNamespace.connect() 方法
     */
    @Override
    public void init() {
        try {
            if (config.isDirectoryNamespace() && config.getRoot().isPresent()) {
                // 调用: LanceNamespace.connect("file", root_path, allocator)
                LOG.info("初始化本地文件系统命名空间: {}", config.getRoot().get());
                namespace = LanceNamespace.connect("file", config.getRoot().get(), allocator);
            } else if (config.isRestNamespace() && config.getUri().isPresent()) {
                // 调用: LanceNamespace.connect("rest", uri, allocator)
                LOG.info("初始化 REST 命名空间: {}", config.getUri().get());
                namespace = LanceNamespace.connect("rest", config.getUri().get(), allocator);
            } else {
                throw new IllegalArgumentException("Invalid namespace configuration");
            }
            
            LOG.info("Lance Namespace 连接成功");
        } catch (Exception e) {
            LOG.error("Failed to initialize Lance Namespace", e);
            throw new RuntimeException("Failed to initialize Lance Namespace", e);
        }
    }
    
    /**
     * 列出所有顶级命名空间
     */
    @Override
    public List<String> listNamespaces() {
        return listNamespaces(new String[0]);
    }
    
    /**
     * 列出父命名空间下的子命名空间
     * 直接调用: LanceNamespace.listNamespaces(ListNamespacesRequest)
     */
    @Override
    public List<String> listNamespaces(String... parentNamespace) {
        try {
            ListNamespacesRequest request = new ListNamespacesRequest();
            if (parentNamespace.length > 0) {
                request.setId(Arrays.asList(parentNamespace));
            }
            
            ListNamespacesResponse response = namespace.listNamespaces(request);
            
            if (response.getNamespaces() != null) {
                Set<String> namespaceSet = response.getNamespaces();
                return new ArrayList<>(namespaceSet);
            }
            return new ArrayList<>();
            
        } catch (Exception e) {
            LOG.warn("Failed to list namespaces", e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 检查命名空间是否存在
     * 直接调用: LanceNamespace.namespaceExists(NamespaceExistsRequest)
     */
    @Override
    public boolean namespaceExists(String... namespaceId) {
        try {
            NamespaceExistsRequest request = new NamespaceExistsRequest();
            request.setId(Arrays.asList(namespaceId));
            
            namespace.namespaceExists(request);
            return true;
        } catch (Exception e) {
            LOG.debug("Namespace does not exist: {}", Arrays.toString(namespaceId));
            return false;
        }
    }
    
    /**
     * 创建命名空间
     * 直接调用: LanceNamespace.createNamespace(CreateNamespaceRequest)
     */
    @Override
    public void createNamespace(Map<String, String> properties, String... namespaceId) {
        try {
            CreateNamespaceRequest request = new CreateNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            
            if (properties != null && !properties.isEmpty()) {
                request.setProperties(properties);
            }
            
            namespace.createNamespace(request);
            
            LOG.info("命名空间创建成功: {}", Arrays.toString(namespaceId));
        } catch (Exception e) {
            LOG.error("Failed to create namespace", e);
            throw new RuntimeException("Failed to create namespace", e);
        }
    }
    
    /**
     * 删除命名空间
     * 直接调用: LanceNamespace.dropNamespace(DropNamespaceRequest)
     */
    @Override
    public void dropNamespace(boolean cascade, String... namespaceId) {
        try {
            DropNamespaceRequest request = new DropNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            request.setCascade(cascade);
            
            namespace.dropNamespace(request);
            
            LOG.info("命名空间删除成功: {}", Arrays.toString(namespaceId));
        } catch (Exception e) {
            LOG.error("Failed to drop namespace", e);
            throw new RuntimeException("Failed to drop namespace", e);
        }
    }
    
    /**
     * 获取命名空间元数据
     * 直接调用: LanceNamespace.describeNamespace(DescribeNamespaceRequest)
     */
    @Override
    public Map<String, String> getNamespaceMetadata(String... namespaceId) {
        try {
            DescribeNamespaceRequest request = new DescribeNamespaceRequest();
            request.setId(Arrays.asList(namespaceId));
            
            DescribeNamespaceResponse response = namespace.describeNamespace(request);
            
            if (response.getProperties() != null) {
                return response.getProperties();
            }
            return new HashMap<>();
        } catch (Exception e) {
            LOG.warn("Failed to get namespace metadata", e);
            return new HashMap<>();
        }
    }
    
    /**
     * 列出命名空间中的所有表
     * 直接调用: LanceNamespace.listTables(ListTablesRequest)
     */
    @Override
    public List<String> listTables(String... namespaceId) {
        try {
            ListTablesRequest request = new ListTablesRequest();
            request.setId(Arrays.asList(namespaceId));
            
            ListTablesResponse response = namespace.listTables(request);
            
            if (response.getTables() != null) {
                Set<String> tableSet = response.getTables();
                return new ArrayList<>(tableSet);
            }
            return new ArrayList<>();
        } catch (Exception e) {
            LOG.warn("Failed to list tables", e);
            return new ArrayList<>();
        }
    }
    
    /**
     * 检查表是否存在
     * 直接调用: LanceNamespace.tableExists(TableExistsRequest)
     */
    @Override
    public boolean tableExists(String... tableId) {
        try {
            TableExistsRequest request = new TableExistsRequest();
            request.setId(Arrays.asList(tableId));
            
            namespace.tableExists(request);
            return true;
        } catch (Exception e) {
            LOG.debug("Table does not exist: {}", Arrays.toString(tableId));
            return false;
        }
    }
    
    /**
     * 创建空表
     * 直接调用: LanceNamespace.createEmptyTable(CreateEmptyTableRequest)
     */
    @Override
    public void createEmptyTable(String location, Map<String, String> properties, String... tableId) {
        try {
            CreateEmptyTableRequest request = new CreateEmptyTableRequest();
            request.setId(Arrays.asList(tableId));
            
            // 设置表位置信息
            if (location != null) {
                request.setPath(location);
            }
            
            namespace.createEmptyTable(request);
            
            LOG.info("表创建成功: {}", Arrays.toString(tableId));
        } catch (Exception e) {
            LOG.error("Failed to create table", e);
            throw new RuntimeException("Failed to create table", e);
        }
    }
    
    /**
     * 删除表
     * 直接调用: LanceNamespace.dropTable(DropTableRequest)
     */
    @Override
    public void dropTable(String... tableId) {
        try {
            DropTableRequest request = new DropTableRequest();
            request.setId(Arrays.asList(tableId));
            
            namespace.dropTable(request);
            
            LOG.info("表删除成功: {}", Arrays.toString(tableId));
        } catch (Exception e) {
            LOG.error("Failed to drop table", e);
            throw new RuntimeException("Failed to drop table", e);
        }
    }
    
    /**
     * 获取表元数据
     * 直接调用: LanceNamespace.describeTable(DescribeTableRequest)
     */
    @Override
    public TableMetadata getTableMetadata(String... tableId) {
        try {
            DescribeTableRequest request = new DescribeTableRequest();
            request.setId(Arrays.asList(tableId));
            
            DescribeTableResponse response = namespace.describeTable(request);
            
            String location = "/path/to/table";
            Map<String, String> options = new HashMap<>();
            
            // 直接调用 API 获取表路径
            if (response.getTable_path() != null) {
                location = response.getTable_path();
            }
            
            // 直接调用 API 获取属性
            if (response.getProperties() != null) {
                options = response.getProperties();
            }
            
            return new TableMetadata(location, options);
        } catch (Exception e) {
            LOG.warn("Failed to get table metadata", e);
            return new TableMetadata("/path/to/table", new HashMap<>());
        }
    }
    

    
    @Override
    public void close() throws Exception {
        try {
            if (namespace != null) {
                namespace.close();
            }
        } catch (Exception e) {
            LOG.warn("Error during namespace cleanup", e);
        }
        
        if (allocator != null) {
            allocator.close();
        }
    }
}
