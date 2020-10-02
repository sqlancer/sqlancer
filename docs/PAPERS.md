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
	year={2020}
}
```
