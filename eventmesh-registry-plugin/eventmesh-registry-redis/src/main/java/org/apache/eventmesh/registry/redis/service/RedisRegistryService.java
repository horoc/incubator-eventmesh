package org.apache.eventmesh.registry.redis.service;

import org.apache.eventmesh.api.exception.RegistryException;
import org.apache.eventmesh.api.registry.RegistryService;
import org.apache.eventmesh.api.registry.dto.EventMeshDataInfo;
import org.apache.eventmesh.api.registry.dto.EventMeshRegisterInfo;
import org.apache.eventmesh.api.registry.dto.EventMeshUnRegisterInfo;
import org.apache.eventmesh.common.config.CommonConfiguration;
import org.apache.eventmesh.common.utils.ConfigurationContextUtil;

import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.redisson.Redisson;
import org.redisson.api.RScript;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisRegistryService implements RegistryService {

    private static final Logger logger = LoggerFactory.getLogger(RedisRegistryService.class);

    private static final AtomicBoolean INIT_STATUS = new AtomicBoolean(false);

    private static final AtomicBoolean START_STATUS = new AtomicBoolean(false);

    private String serverAddr;

    private String username;

    private String password;

    private Map<String, EventMeshRegisterInfo> eventMeshRegisterInfoMap;

    private Redisson redisson;

    private static final String EVENTMESH_LIST_KEY = "event.mesh.register.list";

    @Override
    public void init() throws RegistryException {
        boolean update = INIT_STATUS.compareAndSet(false, true);
        if (!update) {
            return;
        }
        eventMeshRegisterInfoMap = new HashMap<>(ConfigurationContextUtil.KEYS.size());
        for (String key : ConfigurationContextUtil.KEYS) {
            CommonConfiguration commonConfiguration = ConfigurationContextUtil.get(key);
            if (null == commonConfiguration) {
                continue;
            }
            if (StringUtils.isBlank(commonConfiguration.namesrvAddr)) {
                throw new RegistryException("name server address cannot be empty");
            }
            this.serverAddr = commonConfiguration.namesrvAddr;
            this.username = commonConfiguration.eventMeshRegistryPluginUsername;
            this.password = commonConfiguration.eventMeshRegistryPluginPassword;
            break;
        }
    }

    @Override
    public void start() throws RegistryException {

    }

    @Override
    public void shutdown() throws RegistryException {

    }

    @Override
    public List<EventMeshDataInfo> findEventMeshInfoByCluster(String clusterName) throws RegistryException {
        return null;
    }

    @Override
    public List<EventMeshDataInfo> findAllEventMeshInfo() throws RegistryException {
        return null;
    }

    @Override
    public Map<String, Map<String, Integer>> findEventMeshClientDistributionData(String clusterName, String group, String purpose)
        throws RegistryException {
        return null;
    }

    @Override
    public void registerMetadata(Map<String, String> metadataMap) {

    }

    @Override
    public boolean register(EventMeshRegisterInfo eventMeshRegisterInfo) throws RegistryException {
        RScript script = redisson.getScript();
        script.eval()
        return false;
    }

    @Override
    public boolean unRegister(EventMeshUnRegisterInfo eventMeshUnRegisterInfo) throws RegistryException {
        return false;
    }
}