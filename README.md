# ae3.sys.pkg.s4.lcl.bdbje

The on-disk local storage implementation for `ae3.sys.pkg.s4`'s VFS driver, built on Berkeley DB Java Edition: `WorkerBdbj`/`XctBdbj` (worker and transaction), `RowTree`/`RowItem`/`RowTail` (the actual stored row shapes), and a family of `CheckScan*` classes (tree/item/tail/guid integrity scanners used for repair). `bdbj_compare/` holds an alternate/comparison record implementation, kept separate from the main `bdbj/` one.
