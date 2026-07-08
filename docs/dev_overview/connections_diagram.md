<div style="background-color: #838383;">

```{mermaid}
---
title: Connections diagram
---
flowchart TB
	subgraph Mila
		%% direction BT
		cluster_mila@{shape: procs, label: "Mila cluster"}
    client1["SARC (client)"]
    idt["Stats collection"]
	  ldap[LDAP]
	end

	subgraph GCP
		DB[(PostgresQL)]
		sarc@{shape: procs, label: "Jobs"}
		api["API"]
	end

	subgraph DRAC
		cluster_cedar@{shape: procs, label: "Cedar"}
		cluster_beluga@{shape: procs, label: "Beluga"}
		cluster_narval@{shape: procs, label: "Narval"}
		cluster_graham@{shape: procs, label: "Graham"}
	end

  style Mila fill:#662e7d
  style DRAC fill:#d6ab00
  style GCP fill:#1f2123

	DB <==> sarc
	api <==> DB
	sarc -..-> ldap

  sarc-.ssh / sacct.->cluster_mila
  sarc-.ssh / sacct.->cluster_cedar
  sarc-.ssh / sacct.->cluster_beluga
  sarc-.ssh / sacct.->cluster_narval
  sarc-.ssh / sacct.->cluster_graham

  pbi["Power BI"]
  client2["SARC (client)"]
  browser["Client browser"]

	client1-..-api
	client2-..-api
	browser-..-api
	pbi-..-DB
	idt-.Cloud SQL Proxy.-DB
```

</div>
