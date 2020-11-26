# Papers

The testing approaches implemented in SQLancer are described in the three papers below.

## Testing Database Engines via Pivoted Query Synthesis

This paper describes PQS, a testing approach to detect various kinds of logic bugs in DBMS. A preprint is available [here](https://arxiv.org/pdf/2001.04174.pdf).

```
@inproceedings{Rigger2020PQS,
	title = {Testing Database Engines via Pivoted Query Synthesis},
	booktitle = {14th {USENIX} Symposium on Operating Systems Design and Implementation ({OSDI} 20)},
	year = {2020},
	address = {Banff, Alberta},
	url = {https://www.usenix.org/conference/osdi20/presentation/rigger},
	publisher = {{USENIX} Association},
	month = nov,
}
```

## Detecting Optimization Bugs in Database Engines via Non-Optimizing Reference Engine Construction

This paper describes NoREC, a metamorphic testing approach to detect optimization bugs, that is, logic bugs that affect the query optimizer. A preprint is available [here](https://arxiv.org/abs/2007.08292).

```
@inproceedings{Rigger2020NoREC,
	author={Manuel Rigger and Zhendong Su},
	title={{Detecting Optimization Bugs in Database Engines via Non-Optimizing Reference Engine Construction}},
	booktitle = {Proceedings of the 2020 28th ACM Joint Meeting on European Software Engineering Conference and Symposium on the Foundations of Software Engineering},
	series={ESEC/FSE 2020},
	location={Sacramento, California, United States},
	year={2020},
	doi={10.1145/3368089.3409710}
}
```

## Ternary Logic Partitioning: Detecting Logic Bugs in Database Management Systems

This paper describes TLP, a metamorphic testing approach that can detect various kinds of logic bugs and is applicable also to test features such as aggregate functions. A preprint is available [here](https://www.manuelrigger.at/preprints/TLP.pdf).

```
@article{Rigger2020TLP,
	author={Manuel Rigger and Zhendong Su},
	title={Finding Bugs in Database Systems via Query Partitioning},
        journal = {Proc. ACM Program. Lang.},
	number = {OOPSLA},
	year={2020},
	doi={10.1145/3428279},
	volume={4},
	articleno={211}
}
```

# Comparing SQLancer With Other Tools that Find Logic Bugs

If you want to fairly compare other tools with SQLancer, we would be glad to provide feedback (e.g., feel free to send an email to manuel.rigger@inf.ethz.ch). We have the following general recommendations and comments:
* PostgreSQL and SQLite are DBMSs that we comprehensively tested, and where all or most of the bugs that SQLancer could find were fixed. We believe these two systems to be the most challenging test targets. Finding bugs that the approaches implemented in SQLancer overlooked in these systems might thus best demonstrate a new approach's effectiveness. For some other DBMSs like MySQL and MariaDB, SQLancer could still detect unreported bugs; we stopped testing these DBMSs and reporting bugs due to the large number of unfixed bugs.
* We programmatically disabled the generation of features that are likely to trigger known bugs (e.g., see [TiDB](https://github.com/sqlancer/sqlancer/blob/master/src/sqlancer/tidb/TiDBBugs.java)). If a comparison investigates metrics such as code coverage that is achieved when fuzzing a DBMS, it might be desirable to enable the generation of such features.
* For the default SQLite JDBC driver, a number of extensions (e.g., the [soundex function](https://sqlite.org/lang_corefunc.html#soundex)) are disabled by default, which is why they are also disabled by default in the DBMS' options (e.g., see [SQLite3Options](https://github.com/sqlancer/sqlancer/blob/c71b9741f680f4877fc5047445787ed184a5a5e0/src/sqlancer/sqlite3/SQLite3Options.java#L67)). To investigate metrics such as code coverage, it might again be desirable to enable such options.
* The maximum expression depth (see the `--max-expression-depth` option), the number of queries issued per database (see the `--num-queries` option), and the number of tables and views that are created (currently, SQLancer does not have an option to set these) significantly influence the tool's effectiveness and performance characteristics. It might be desirable to experiment with different values for the expression depth (e.g., values between 2 and 4), the number of queries (1000-100,000), as well as the number of tables and views.
