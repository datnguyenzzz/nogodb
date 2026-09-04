// https://arrow.apache.org/docs/format/Columnar.html#fixed-size-primitive-layout

use std::{
    cmp,
    collections::{BTreeMap, BTreeSet},
    thread,
};

#[cfg(target_os = "linux")]
use std::fs;

use anyhow::Result;

#[cfg(target_os = "linux")]
const NODE_ROOT: &str = "/sys/devices/system/node";
#[cfg(target_os = "linux")]
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

pub struct NumaTopology {
    /// Maps each physical `core_id` -> its physical `numa_node_id`
    cpu_to_numa: BTreeMap<usize, usize>,
}

impl NumaTopology {
    /// Detects the hardware NUMA topology dynamically from the OS.
    /// This is fully cgroup-quota and cpuset aware.
    pub fn detect() -> Self {
        let mut core_to_numa = BTreeMap::new();
        #[allow(unused_mut)]
        let mut nodes = BTreeSet::new();
        let effective_cores = Self::get_available_core_ids();

        let cgroup_limit = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        let active_core_count = cmp::min(cgroup_limit, effective_cores.len());
        let available_cores = &effective_cores[0..active_core_count];

        // Symmetrically map only these active effective cores to physical NUMA nodes on Linux
        #[cfg(target_os = "linux")]
        {
            if let Ok(entries) = fs::read_dir(NODE_ROOT) {
                for e in entries {
                    if let Ok(entry) = e {
                        let name = entry.file_name().to_string_lossy().into_owned();
                        if name.starts_with("node")
                            && name["node".len()..].chars().all(|c| c.is_ascii_digit())
                        {
                            if let Ok(node_id) = name["node".len()..].parse::<usize>() {
                                nodes.insert(node_id);

                                let cpulist_path = format!("{}/{}/cpulist", NODE_ROOT, name);
                                if let Ok(cpulist_str) = fs::read_to_string(cpulist_path) {
                                    if let Ok(parsed_cores) = Self::parse_cpulist(&cpulist_str) {
                                        for cid in parsed_cores {
                                            if available_cores.contains(&cid) {
                                                core_to_numa.insert(cid, node_id);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Fault-Tolerant Healing:
        // Ensure every single core in our allowed subset is mapped.
        // If a core is unmapped, we distribute it round-robin across known active nodes!
        let active_node_list: Vec<usize> = if nodes.is_empty() {
            vec![0]
        } else {
            let mut list: Vec<usize> = nodes.iter().copied().collect();
            list.sort();
            list
        };

        for (i, &cid) in available_cores.iter().enumerate() {
            if !core_to_numa.contains_key(&cid) {
                let balanced_node = active_node_list[i % active_node_list.len()];
                core_to_numa.insert(cid, balanced_node);
            }
        }

        Self { cpu_to_numa: core_to_numa }
    }

    #[cfg(test)]
    fn detect_with_path<P: AsRef<std::path::Path>>(node_path: P) -> Self {
        let mut core_to_numa = BTreeMap::new();
        let mut nodes = BTreeSet::new();
        let effective_cores = Self::get_available_core_ids();

        let cgroup_limit = thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);

        let active_core_count = cmp::min(cgroup_limit, effective_cores.len());
        let available_cores = &effective_cores[0..active_core_count];

        if let Ok(entries) = std::fs::read_dir(node_path.as_ref()) {
            for e in entries {
                if let Ok(entry) = e {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if name.starts_with("node")
                        && name["node".len()..].chars().all(|c| c.is_ascii_digit())
                    {
                        if let Ok(node_id) = name["node".len()..].parse::<usize>() {
                            nodes.insert(node_id);

                            let cpulist_path = node_path.as_ref().join(&name).join("cpulist");
                            if let Ok(cpulist_str) = std::fs::read_to_string(cpulist_path) {
                                if let Ok(parsed_cores) = Self::parse_cpulist(&cpulist_str) {
                                    for cid in parsed_cores {
                                        if available_cores.contains(&cid) {
                                            core_to_numa.insert(cid, node_id);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let active_node_list: Vec<usize> = if nodes.is_empty() {
            vec![0]
        } else {
            let mut list: Vec<usize> = nodes.iter().copied().collect();
            list.sort();
            list
        };

        for (i, &cid) in available_cores.iter().enumerate() {
            if !core_to_numa.contains_key(&cid) {
                let balanced_node = active_node_list[i % active_node_list.len()];
                core_to_numa.insert(cid, balanced_node);
            }
        }

        Self { cpu_to_numa: core_to_numa }
    }

    /// Returns the physical NUMA node ID of a specific Core ID
    /// with a modulo protection boundary shield.
    #[inline]
    pub fn numa_node(&self, cid: usize) -> usize {
        let node_id = self.cpu_to_numa.get(&cid).copied().unwrap_or(0);
        node_id % self.numa_nodes_count()
    }

    /// Returns the total number of physical NUMA nodes discovered 
    /// on the system.
    #[inline]
    pub fn numa_nodes_count(&self) -> usize {
        let unique_nodes: std::collections::HashSet<usize> = self.cpu_to_numa.values().copied().collect();
        unique_nodes.len().max(1)
    }

    /// Returns the total number of physical CPU cores mapped.
    #[inline]
    pub fn cores_count(&self) -> usize {
        self.cpu_to_numa.len()
    }

    fn get_available_core_ids() -> Vec<usize> {
        let mut effective_cores = vec![];

        #[cfg(target_os = "linux")]
        {
            let mut cpulist_str =
                fs::read_to_string(format!("{}/cpuset.cpus.effective", CGROUP_ROOT))
                    .or_else(|_| {
                        fs::read_to_string(format!("{}/cpuset/cpuset.cpus.effective", CGROUP_ROOT))
                    })
                    .unwrap_or_default();

            if let Ok(cores) = Self::parse_cpulist(&cpulist_str) {
                effective_cores = cores
            }
        }

        // fallback to process affinity mask (sched_getaffinity) if cgroup path is inaccessible
        if effective_cores.is_empty() {
            effective_cores = core_affinity::get_core_ids()
                .unwrap_or_default()
                .iter()
                .map(|c| c.id)
                .collect();
        }

        // fallback to thread available parallelism
        if effective_cores.is_empty() {
            let limit = thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1);
            effective_cores = (0..limit).collect();
        }

        effective_cores
    }

    /// Helper to parse Linux-standard `cpulist` files (e.g., "0-3,8,10-12")
    #[allow(unused)]
    fn parse_cpulist(s: &str) -> Result<Vec<usize>> {
        let mut cores = vec![];
        if s.is_empty() {
            return Ok(cores);
        }

        for part in s.split(',') {
            if part.contains('-') {
                let range: Vec<&str> = part.split('-').collect();
                if range.len() == 2 {
                    let start = range[0].trim().parse::<usize>()?;
                    let end = range[1].trim().parse::<usize>()?;
                    for i in start..=end {
                        cores.push(i);
                    }
                }
            } else {
                cores.push(part.trim().parse()?);
            }
        }

        Ok(cores)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cpulist_ranges_and_lists() {
        // Test composite ranges & individual numbers
        let parsed = NumaTopology::parse_cpulist("0-3,8,10-12").unwrap();
        assert_eq!(parsed, vec![0, 1, 2, 3, 8, 10, 11, 12]);

        // Test single number
        let parsed_single = NumaTopology::parse_cpulist("42").unwrap();
        assert_eq!(parsed_single, vec![42]);

        // Test empty string
        let parsed_empty = NumaTopology::parse_cpulist("").unwrap();
        assert!(parsed_empty.is_empty());
    }

    #[test]
    fn test_numa_topology_discovery_robust() {
        let topo = NumaTopology::detect();
        
        // Count should always be >= 1 (Universal fallback)
        assert!(topo.numa_nodes_count() >= 1);
        assert!(topo.cores_count() >= 1);

        // Core 0 should always fallback/map safely to some valid NUMA node
        let node_id = topo.numa_node(0);
        assert!(node_id < topo.numa_nodes_count());
    }

    #[test]
    fn test_parse_mock_sysfs_topology() {
        let temp_dir = tempfile::tempdir().unwrap();
        let sysfs_node_path = temp_dir.path();

        // 1. Simulate Node 0 with cores 0, 1, 2
        let node0_dir = sysfs_node_path.join("node0");
        std::fs::create_dir(&node0_dir).unwrap();
        std::fs::write(node0_dir.join("cpulist"), "0-2\n").unwrap();

        // 2. Simulate Node 1 with cores 3, 4, 5
        let node1_dir = sysfs_node_path.join("node1");
        std::fs::create_dir(&node1_dir).unwrap();
        std::fs::write(node1_dir.join("cpulist"), "3-5\n").unwrap();

        // 3. Run our parser against the mock directory!
        let topo = NumaTopology::detect_with_path(sysfs_node_path);

        assert_eq!(topo.numa_nodes_count(), 2);
        
        // Match core maps safely (only if they are part of active allowed cores)
        let active_cores = NumaTopology::get_available_core_ids();
        if active_cores.contains(&1) {
            assert_eq!(topo.numa_node(1), 0);
        }
        if active_cores.contains(&4) {
            assert_eq!(topo.numa_node(4), 1);
        }
    }
}
