# tmip-emat-bespoke-demo

This repository contains a demo module for creating a files-based core model interface.
This kind of interface is perfectly reasonable for use with the default TMIP-EMAT
repository. If you do not need to edit any of the core functionality of TMIP-EMAT, then
you do not need to "fork" the original repository.  Instead, you can install those tools
simply using `conda`, and write just a single add-on interface file for your particular
core model, as shown in this repository.

The interface for this demo is defined in the python module `core_files_demo.py`. This
heavily commented file shows all of the required parts of an interface, and offers a few 
different approaches for how to build that interface.

The Jupyter notebook `interface-walkthrough.ipynb` provides a short demo that walks through
developing and using the interface, including single runs, multiple experiments, and 
parallel processes.
