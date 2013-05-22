package storm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.scheduler.Cluster;
import backtype.storm.scheduler.EvenScheduler;
import backtype.storm.scheduler.ExecutorDetails;
import backtype.storm.scheduler.IScheduler;
import backtype.storm.scheduler.SupervisorDetails;
import backtype.storm.scheduler.Topologies;
import backtype.storm.scheduler.TopologyDetails;
import backtype.storm.scheduler.WorkerSlot;

public class SetScheduler implements IScheduler {

    private static Logger logger = Logger.getLogger("setscheduler");

    /**
     * 结构说明： SupervisorName - Map(SupervisorId - List(Port))
     */
    private Hashtable<String, Map<String, List<Integer>>> activeSetVisors;
    private String[] supervisorNames = { "logparser" };

    public SetScheduler() {

        logger.info("constructing set scheduler...");
        this.activeSetVisors = new Hashtable<String, Map<String, List<Integer>>>();
        for (String name : this.supervisorNames) {
            Hashtable<String, List<Integer>> supervisor = new Hashtable<String, List<Integer>>();
            this.activeSetVisors.put(new String(name), supervisor);
        }
    }

    protected void printActiveSetVisors() {

        for (String k : activeSetVisors.keySet()) {
            Map<String, List<Integer>> supervisor = activeSetVisors.get(k);
            for (String k1 : supervisor.keySet()) {
                logger.info("active set visors:" + k + "-" + k1 + "-" + supervisor.get(k1));
            }
        }
    }

    /**
     * 获取在集群中存在的set supervisors
     * 
     * @param supervisors
     * @return
     */
    protected Map<String, List<SupervisorDetails>> getAvailableSetVisors(
            Collection<SupervisorDetails> supervisors) {

        Hashtable<String, List<SupervisorDetails>> availableSetVisors = new Hashtable<String, List<SupervisorDetails>>();
        // 过滤出可用的set supervisors
        for (SupervisorDetails supervisor : supervisors) {

            Map meta = (Map) supervisor.getSchedulerMeta();
            String metaName = null;
            if (meta != null) {
                metaName = (String) meta.get("name");
            }

            for (String SupervisorName : this.activeSetVisors.keySet()) {

                // 采用set supervisor name完全匹配 supervisor配置项 -
                // supervisor.scheduler.meta:name - "SupervisorName"
                if (metaName != null && metaName.equals(SupervisorName)) {

                    List<SupervisorDetails> availableSetVisorList = availableSetVisors.get(SupervisorName);
                    if (availableSetVisorList == null) {
                        availableSetVisorList = new ArrayList<SupervisorDetails>();
                    }
                    availableSetVisorList.add(supervisor);
                    availableSetVisors.put(SupervisorName, availableSetVisorList);
                    break;
                }
            }
        }
        return availableSetVisors;
    }

    /**
     * 清除不可用的slot
     * 
     * @param supervisors
     * @param cluster
     */
    protected void clearUnavailableSetSlot(
            Map<String, List<Integer>> supervisors, Cluster cluster) {

        for (String SupervisorId : supervisors.keySet()) {

            SupervisorDetails supervisor = cluster.getSupervisorById(SupervisorId);
            // 集群中没有该set supervisor
            if (supervisor == null)
                continue;

            List<Integer> ports = supervisors.get(SupervisorId);
            // 对应的set supervisor尚未空
            if (ports.isEmpty())
                continue;

            List<Integer> usedPorts = cluster.getUsedPorts(supervisor);
            List<Integer> availablePorts = cluster.getAvailablePorts(supervisor);
            List<Integer> freePorts = new ArrayList<Integer>();

            // 过滤出对应set supervisor中不存在的port
            for (Integer port : ports) {

                WorkerSlot slot = new WorkerSlot(SupervisorId, port);
                if (!cluster.isSlotOccupied(slot)
                    || (!usedPorts.contains(port) && !availablePorts.contains(port))) {
                    freePorts.add(port);
                }
            }

            // 删除不存在的port
            for (Integer port : freePorts) {
                ports.remove(port);
            }
        }
    }

    /**
     * 清除不可用的set supervisor
     * 
     * @param availableSetVisors
     * @param cluster
     */
    protected void clearUnavailableSetVisor(
            Map<String, List<SupervisorDetails>> availableSetVisors,
            Cluster cluster) {

        for (String SupervisorName : this.activeSetVisors.keySet()) {

            Map<String, List<Integer>> supervisors = this.activeSetVisors.get(SupervisorName);
            if (supervisors.isEmpty())
                continue;

            // 清除集群中不存在set supervisor对应的port
            if (!availableSetVisors.containsKey(SupervisorName)) {
                supervisors.clear();
            } else {
                this.clearUnavailableSetSlot(supervisors, cluster);
            }
        }
    }

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {

        Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
        Map<String, List<SupervisorDetails>> availableSetVisors = this.getAvailableSetVisors(supervisors);

        printActiveSetVisors();
        this.clearUnavailableSetVisor(availableSetVisors, cluster);

        if (!availableSetVisors.isEmpty()) {

            Collection<TopologyDetails> topologiesCollection = topologies.getTopologies();
            for (TopologyDetails topology : topologiesCollection) {

                // 如果该topology已经分配
                if (!cluster.needsScheduling(topology))
                    continue;

                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);
                // 过滤出component名与set supervisor name前缀匹配的组件
                // 然后将该组件分配一个空闲的set slot
                for (String componentName : componentToExecutors.keySet()) {

                    List<SupervisorDetails> setVisors = null;
                    String setVisorName = null;

                    // 过滤出与组件名匹配的set supervisors
                    for (String SupervisorName : availableSetVisors.keySet()) {
                        if (componentName.startsWith(SupervisorName + "-")) {
                            setVisorName = SupervisorName;
                            setVisors = availableSetVisors.get(SupervisorName);
                            break;
                        }
                    }

                    // 存在匹配的component
                    if (setVisors != null) {

                        // 获取当前已经被分配的set slot
                        Map<String, List<Integer>> activeSetVisorList = this.activeSetVisors.get(setVisorName);
                        WorkerSlot workerSlot = null;

                        // 从匹配的set supervisors中选取一个空闲的slot
                        for (SupervisorDetails supervisor : setVisors) {

                            List<WorkerSlot> availableWorkSlotList = cluster.getAvailableSlots(supervisor);
                            // 如果有空闲的slot,则直接分配给component
                            if (!availableWorkSlotList.isEmpty()) {
                                workerSlot = availableWorkSlotList.get(0);
                                break;
                            }
                            // 否则选取第一个非活跃的set slot分配给component
                            else if (workerSlot == null) {

                                String supervisorId = supervisor.getId();
                                List<Integer> ports = null;
                                if (activeSetVisorList != null) {
                                    ports = activeSetVisorList.get(supervisorId);
                                }

                                // 从当前被占用的port中选择第一个非活跃的set slot
                                for (Integer port : cluster.getUsedPorts(supervisor)) {
                                    WorkerSlot slot = new WorkerSlot(supervisorId, port);

                                    if (ports == null || !ports.contains(port)) {
                                        cluster.freeSlot(slot);
                                        workerSlot = slot;
                                        break;
                                    }
                                }
                            }
                        }

                        List<ExecutorDetails> executors = componentToExecutors.get(componentName);
                        // 如果存在空闲的set slot,否则输出日志
                        if (workerSlot != null) {

                            // 将被分配的set slot存放在活跃set supervisor中
                            String supervisorId = workerSlot.getNodeId();
                            if (activeSetVisorList == null) {
                                activeSetVisorList = new Hashtable<String, List<Integer>>();
                            }

                            if (activeSetVisorList.get(supervisorId) == null) {
                                activeSetVisorList.put(new String(supervisorId), new ArrayList<Integer>());
                            }
                            activeSetVisorList.get(supervisorId).add(workerSlot.getPort());
                            this.activeSetVisors.put(setVisorName, activeSetVisorList);
                            logger.info("chose SupervisorId:" + supervisorId + " port:" + workerSlot.getPort() + " for " + executors);

                            // 分配set slot给component
                            cluster.assign(workerSlot, topology.getId(), executors);
                        } else {
                            logger.warn("no available slot for " + executors);
                        }
                    }
                }
            }
        }
        printActiveSetVisors();
        new EvenScheduler().schedule(topologies, cluster);
    }
}
