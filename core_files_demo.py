
import tempfile
import os
import re
import pandas as pd
import numpy as np
import shutil
from distutils.dir_util import copy_tree
import platform
import subprocess
import uuid
import time

from emat import Scope
from emat import SQLiteDB
from emat.model.core_files import FilesCoreModel
from emat.model.core_files.parsers import TableParser, MappingParser, loc, key

import logging
_logger = logging.getLogger("EMAT.DEMO")

# The demo model code is located in the same
# directory as this script file.  We can recover
# this directory name like this, even if the
# current working directory is different.
# In your application, you may want to program
# this differently, possibly hard-coding the name
# of the model directory.
this_directory = os.path.dirname(__file__)


def join_norm(*args):
	return os.path.normpath(os.path.join(*args))

def to_simple_python(v):
	"""
	Convert a value to a simple Python value, which helps clean up YAML output.
	"""
	try:
		isfloat = np.issubdtype(v, np.floating)
	except:
		isfloat = False
	try:
		isint = np.issubdtype(v, np.integer)
	except:
		isint = False
	if isfloat:
		return float(v)
	elif isint:
		return int(v)
	else:
		return v


class ReplacementOfNumber:
	"""
	This class provides a mechanism to edit a text file, replacing
	a the numerical value of a particular parameter with a new value.

	This implementation uses "regular expressions"
	https://en.wikipedia.org/wiki/Regular_expression
	to find and replace assignment operations in the text file being
	manipulated.  An advantage of this approach is that the source
	file that contains the script to be modified can start off in
	a "runnable" default, which can be used independently of
	TMIP-EMAT.
	"""
	numbr = r"([-+]?\d*\.?\d*[eE]?[-+]?\d*|\d+\/\d+)"  # matches any number representation
	def __init__(self, varname, assign_operator=":", logger=None):
		self.varname = varname
		# In this example, we use `re.compile` to create a tool that will
		# search through a text file, finding instances of the general
		# form "varname: 123.456", and be able to replace the value
		# 123.456 with some other number. The assignment_operator in
		# this example is set to the colon character, as that's what
		# is used in YAML files which are used in this demo, but it can
		# be replaced with "=" or "<-" or whatever assignment operator is
		# used in the text of the file being modified.
		self.regex = re.compile(f"({varname}\s*{assign_operator}\s*)({self.numbr})")
		self.logger = logger
	def sub(self, value, s):
		"""
		Find and replace all instances of the variable assignment in a string.

		Args:
			value (numeric):
				The new value to insert.
			s (str):
				The string to manipulate. This is generally the complete
				text of a script file of some kind that has already been
				loaded into memory.

		Returns:
			s (str): The edited version of the input string.
		"""
		s, n = self.regex.subn(f"\g<1>{value}", s)
		if self.logger is not None:
			self.logger.info(f"For '{self.varname}': {n} substitutions made")
		return s


class ReplacementOfString:
	"""
	This class provides a mechanism to edit a text file, replacing
	the string value of a particular parameter with a new value.
	The regular expression used to find and replace the strings is
	different, but the fundamental approach is the same as for the
	`ReplacementOfNumber` above.
	"""
	def __init__(self, varname, assign_operator=":", logger=None):
		self.varname = varname
		self.regex = re.compile(f"({varname}\s*{assign_operator}\s*)([^#\n]*)(#.*)?", flags=re.MULTILINE)
		self.logger = logger
	def sub(self, value, s):
		"""
		Find and replace all instances of the variable assignment in a string.

		Args:
			value (str):
				The new value to insert.
			s (str):
				The string to manipulate. This is generally the complete
				text of a script file of some kind that has already been
				loaded into memory.

		Returns:
			s (str): The edited version of the input string.
		"""
		# This implementation of the replacement algorithm preserves
		# comments appended after the value using the hash # character.
		s, n = self.regex.subn(f"\g<1>{value}  \g<3>", s)
		if self.logger is not None:
			self.logger.info(f"For '{self.varname}': {n} substitutions made")
		return s


class RoadTestFileModel(FilesCoreModel):
	"""
	A demo class for using the Road Test as a file reading core model.

	The base class for this model class includes a number of arguments
	in the `__init__` method to declare configuration, scope, and name,
	but for this example those values are hard-coded into the the
	`__init__` method.  Only the database `db` argument is exposed to
	the user, which allows for (re)using a persistent database.

	Args:
		db (emat.Database):
			An optional Database to store experiments and results.
			This allows this demo to store results in a persistent
			manner across sessions.  If a `db` is not given, one is
			created and initialized in the temporary directory
			alongside the other demo model files, but it will be
			deleted automatically when the Python session ends.
		scope_file (Path, optional):
			The file name of the scope file to use.  A default
			scope is included if none is given.
	"""

	def __init__(self, db=None, scope_file=None):

		# Make a temporary directory for this example
		# A 'real' core models application may want to use a
		# more permanent directory.  The temporary directory
		# solution works well if (a) the total filesize of files
		# needed to run the file is manageable, and (b) you don't
		# need to inspect these files later for any reason
		# (debugging, etc.).
		self.master_directory = tempfile.TemporaryDirectory()
		os.chdir(self.master_directory.name)
		_logger.warning(f"changing cwd to {self.master_directory.name}")
		cwd = self.master_directory.name

		# Housekeeping for this example:
		# Also copy the CONFIG and SCOPE files
		shutil.copy2(
			join_norm(this_directory, 'core-model-files', f"road-test-model-config.yml"),
			join_norm(cwd, f"road-test-model-config.yml"),
		)
		if scope_file is None:
			scope_file = join_norm(this_directory, 'core-model-files', f"road-test-scope.yml")
		shutil.copy2(
			scope_file,
			join_norm(cwd, f"road-test-scope.yml"),
		)
		shutil.copy2(
			join_norm(this_directory, f"road-test-colleague.sqlitedb"),
			join_norm(cwd, f"road-test-colleague.sqlitedb"),
		)

		scope = Scope(join_norm(cwd, "road-test-scope.yml"))

		if db is None:
			db = SQLiteDB(
				join_norm(cwd, "road-test-demo.db"),
				initialize=True,
			)
		if db is False: # explicitly use no DB
			db = None
		else:
			if scope.name not in db.read_scope_names():
				db.store_scope(scope)

		# Initialize the super class (FilesCoreModel)
		super().__init__(
			configuration=join_norm(cwd, "road-test-model-config.yml"),
			scope=scope,
			db=db,
			name='RoadTestFilesModel',
			local_directory = cwd,
		)

		# Populate the model_path directory of the files-based model.
		# Depending on how large your core model is, you may or may
		# not want to be copying the whole thing.  As an alternative,
		# you can work in the original directory, but just be careful
		# not to do anything destructive to files that are not otherwise
		# backed up elsewhere.
		copy_tree(
			join_norm(this_directory, 'core-model-files', self.model_path),
			join_norm(cwd, self.model_path),
		)

		# If this files-based model is serialized to multiple worker copies,
		# the local directory for each copy will be different.  But we
		# want all copies to point back to a common archive, so we'll
		# convert the archive path to an absolute path here, before
		# we fork the worker copies.
		self.archive_path = os.path.abspath(self.resolved_archive_path)

		# Add parsers to instruct the load_measures function
		# how to parse the outputs and get the measure values.
		self.add_parser(
			TableParser(
				"output_1.csv.gz",
				{
					'value_of_time_savings': loc['plain', 'value_of_time_savings'],
					'present_cost_expansion': loc['plain', 'present_cost_expansion'],
					'cost_of_capacity_expansion': loc['plain', 'cost_of_capacity_expansion'],
					'net_benefits': loc['plain', 'net_benefits'],
				},
				index_col=0,
			)
		)
		self.add_parser(
			MappingParser(
				"output.yaml",
				{
					'build_travel_time': key['build_travel_time'],
					'no_build_travel_time': key['no_build_travel_time'],
					'time_savings': key['time_savings'],
				}
			)
		)

	def enter_run_model(self):
		"""
		Initiate run timing for logging.
		"""
		self._start_time = time.time()

	def exit_run_model(self):
		"""
		Complete run timing for logging.

		This demo function logs the elapsed time for every model run.
		This is not required for normal operation, but may be interesting
		or help debug problems in real applications.
		"""
		self._end_time = time.time()
		elapsed = self._end_time - self._start_time
		self.log(f"RAN EXPERIMENT IN {elapsed:.2f} SECONDS")

	def setup(self, params):
		"""
		Configure the demo core model with the experiment variable values.

		This method is the place where the core model set up takes place,
		including creating or modifying files as necessary to prepare
		for a core model run.  When running experiments, this method
		is called once for each core model experiment, where each experiment
		is defined by a set of particular values for both the exogenous
		uncertainties and the policy levers.  These values are passed to
		the experiment only here, and not in the `run` method itself.
		This facilitates debugging, as the `setup` method can potentially
		be used without the `run` method, allowing the user to manually
		inspect the prepared files and ensure they are correct before
		actually running a potentially expensive model.

		Each input exogenous uncertainty or policy lever can potentially
		be used to manipulate multiple different aspects of the underlying
		core model.  For example, a policy lever that includes a number of
		discrete future network "build" options might trigger the replacement
		of multiple related network definition files.  Or, a single uncertainty
		relating to the cost of fuel might scale both a parameter linked to
		the modeled per-mile cost of operating an automobile, as well as the
		modeled total cost of fuel used by transit services.

		At the end of the `setup` method, a core model experiment should be
		ready to run using the `run` method.

		Args:
			params (dict):
				experiment variables including both exogenous
				uncertainty and policy levers

		Raises:
			KeyError:
				if a defined experiment variable is not supported
				by the core model
		"""

		# We'll create a new unique id for this model upon a fresh
		# SETUP event.  This is not strictly necessary for a small
		# example such as this, but it may be useful in larger
		# implementations to keep track of what directory is attached
		# to a given experiment.
		# self.uid = uuid.uuid1()

		super().setup(params)
		self.log(f"RoadTestFileModel SETUP RUNID-{self.run_id}")

		# Check if we are using distributed multi-processing. If so,
		# we'll need to copy some files into a local working directory,
		# as otherwise changes in the files will over-write each other
		# when different processes are working in a common directory at
		# the same time.
		try:
			# First try to import the dask.distributed library
			# and check if this code is running on a worker.
			from dask.distributed import get_worker
			worker = get_worker()
		except (ValueError, ImportError):
			# If the library is not available, or if the code is
			# not running on a worker, then we are not running
			# in multi-processing mode, and we can just use
			# the main cwd as the working directory without
			# copying anything.
			pass
		else:
			# If we do find we are running this setup on a
			# worker, then we want to set the local directory
			# accordingly. We copy model files from the "master"
			# working directory to the worker's local directory,
			# if it is different (it should be). Depending
			# on how large your core model is, you may or may
			# not want to be copying the whole thing.
			if self.local_directory != worker.local_directory:
				_logger.debug(f"DISTRIBUTED.COPY FROM {self.local_directory}")
				_logger.debug(f"                   TO {worker.local_directory}")
				copy_tree(
					join_norm(self.local_directory, self.model_path),
					join_norm(worker.local_directory, self.model_path),
				)
				self.local_directory = worker.local_directory

		# Set default experiment_id as the run_id integer
		experiment_id = self.run_id.int

		# Write params and experiment_id to folder, if possible
		try:
			try:
				import yaml as serializer
			except ImportError:
				import json as serializer
			simple_params = {k:to_simple_python(v) for k,v in params.items()}
			with open(join_norm(self.local_directory,"_emat_parameters_.yml"), 'w') as fstream:
				serializer.dump(simple_params, fstream)
			db = getattr(self, 'db', None)
			if db is not None:
				experiment_id = db.get_experiment_id(self.scope.name, None, params)
			with open(join_norm(self.local_directory,"_emat_experiment_id_.yml"), 'w') as fstream:
				serializer.dump({
					'experiment_id':experiment_id,
					'run_id':self.run_id,
				}, fstream)
		except:
			_logger.exception("error in serializing parameters")

		# The process of manipulating each input file is broken out
		# into discrete sub-methods, as each step is loosely independent
		# and having seperate methods makes this clearer.
		self._manipulate_input_file_1(params)
		self._manipulate_input_file_2(params)

		# Log to the database if available
		self.log(f"RoadTestFileModel SETUP complete experiment_id {experiment_id} RUNID-{self.run_id}")

	def _manipulate_input_file_1(self, params):
		"""
		Prepare the levers input file based on the existing file.

		The first file used to set parameter values for this demo is in
		a ready-to-use format. This file has default values pre-coded into
		the file, and needs to be parsed to find the places to insert the
		replacement values for a particular experiment. This can be done
		using regular expressions (as in this demo), or any other method you
		like to edit the file appropriately.  The advantage of this approach
		is that the base file is ready to use with the core model as-is,
		facilitating the use of this file outside the EMAT context.

		Args:
			params (dict):
				The parameters for this experiment, including both
				exogenous uncertainties and policy levers.
		"""

		numbers_to_levers_file = [
			'expand_capacity',
			'amortization_period',
			'interest_rate_lock',
			'lane_width',
			'mandatory_unused_lever',
		]

		strings_to_levers_file = [
			'debt_type',
		]

		# load the text of the first demo input file
		with open(join_norm(self.local_directory, self.model_path, 'demo-inputs-l.yml'), 'rt') as f:
			y = f.read()

		# use regex to manipulate the content, inserting the defined
		# parameter values
		for n in numbers_to_levers_file:
			if n in params:
				y = ReplacementOfNumber(n).sub(params[n], y)
		for s in strings_to_levers_file:
			if s in params:
				y = ReplacementOfString(s).sub(params[s], y)

		# write the manipulated text back out to the first demo input file
		with open(join_norm(self.local_directory, self.model_path, 'demo-inputs-l.yml'), 'wt') as f:
			f.write(y)

	def _manipulate_input_file_2(self, params):
		"""
		Prepare the uncertainties input file based on a template.

		The second file used to set parameter values for this demo is in a template format.
		Each value to be set is indicated in the file by a unique token that is easy to
		search and replace, and definitely not something that appear in any script otherwise.
		This approach makes the text-substitution code that is used in this module much
		simpler and less prone to bugs.  But there is a small downside of this approach:
		every parameter must definitely be replaced in this process, as the template file
		is unusable unless every unique token is replaced.

		Args:
			params (dict):
				The parameters for this experiment, including both
				exogenous uncertainties and policy levers.
		"""

		# The file template we will manipulate for this demo contains eight unique tokens that
		# we will need to replace in the file, and they are listed here.
		tokens_in_file = [
			'alpha',
			'beta',
			'input_flow',
			'value_of_time',
			'labor_unit_cost_expansion',
			'materials_unit_cost_expansion',
			'interest_rate',
			'yield_curve',
		]

		# Six of these eight tokens align exactly with the scoped model parameters,
		# and when we insert the value into the file we can simply pass the that value
		# directly from the `params` input to this method.  However, two of the tokens
		# in the file are *not* exactly the same: the core model input file requires
		# the unit cost of expansion to be split across labor and materials.  These two
		# tokens both connect back to the more generic 'unit_cost_expansion' parameter
		# that appears in the TMIP-EMAT model scope. To accommodate this, we will
		# manipulate the values here in Python, so the one input parameter is used to
		# populate both core model inputs.
		computed_params = params.copy()
		computed_params['labor_unit_cost_expansion'] = params['unit_cost_expansion'] * 0.6
		computed_params['materials_unit_cost_expansion'] = params['unit_cost_expansion'] * 0.4

		# Now, we load the text of the second demo input file into a string in memory
		with open(join_norm(self.local_directory, self.model_path, 'demo-inputs-x.yml.template'), 'rt') as f:
			y = f.read()

		# Loop over all the tokens in the file, replacing them with usable values,
		# or triggering an error if we cannot.
		for n in tokens_in_file:
			if n in computed_params:
				# No regex here, just a simple string replacement.  Note the replacement
				# value also must itself be a string.
				y = y.replace(
					f"__EMAT_PROVIDES_VALUE__{n.upper()}__",  # the token to replace
					str(computed_params[n])  # the value to replace it with (as a string)
				)
			else:
				# Raise an error now if one of the required parameters is missing, to
				# save us the trouble of having the error crop up later, because it will.
				raise ValueError(f'missing required parameter "{n}"')

		# Write the manipulated text back out to the second demo input file.  We don't write
		# to the template file, but to the expected normal filename for our script.
		with open(join_norm(self.local_directory, self.model_path, 'demo-inputs-x.yml'), 'wt') as f:
			f.write(y)

	def run(self):
		"""
		Run the core model.

		This method is the place where the core model run takes place.
		Note that this method takes no arguments; all the input
		exogenous uncertainties and policy levers are delivered to the
		core model in the `setup` method, which will be executed prior
		to calling this method. This facilitates debugging, as the `setup`
		method can potentially be used without the `run` method, allowing
		the user to manually inspect the prepared files and ensure they
		are correct before actually running a potentially expensive model.
		When running experiments, this method is called once for each core
		model experiment, after the `setup` method completes.

		If the core model requires some post-processing by `post_process`
		method defined in this API, then when this function terminates
		the model directory should be in a state that is ready to run the
		`post_process` command next.

		Raises:
		    UserWarning: If model is not properly setup
		"""
		self.log("RoadTestFileModel RUN ...", level=logging.DEBUG)

		# This demo uses the `emat-road-test-demo` command line tool
		# that is installed automatically when TMIP-EMAT is installed,
		# but the name of the tool on Windows also includes `.exe`.
		if platform.system() == 'Windows':
			cmd = 'emat-road-test-demo.exe'
		else:
			cmd = 'emat-road-test-demo'

		# The subprocess.run command runs a command line tool. The
		# name of the command line tool, plus all the command line arguments
		# for the tool, are given as a list of strings, not one string.
		# The `cwd` argument sets the current working directory from which the
		# command line tool is launched.  Setting `capture_output` to True
		# will capture both stdout and stderr from the command line tool, and
		# make these available in the result to facilitate debugging.
		self.last_run_result = subprocess.run(
			[cmd, '--uncs', 'demo-inputs-x.yml', '--levers', 'demo-inputs-l.yml'],
			cwd=join_norm(self.local_directory, self.model_path),
			capture_output=True,
		)
		if self.last_run_result.returncode:
			raise subprocess.CalledProcessError(
				self.last_run_result.returncode,
				self.last_run_result.args,
				self.last_run_result.stdout,
				self.last_run_result.stderr,
			)

		self.log(f"RoadTestFileModel RUN complete RUNID-{self.run_id}")

	def last_run_logs(self, output=None):
		"""
		Display the logs from the last run.
		"""
		if output is None:
			output = print
		def to_out(x):
			if isinstance(x, bytes):
				output(x.decode())
			else:
				output(x)
		try:
			last_run_result = self.last_run_result
		except AttributeError:
			output("no run stored")
		else:
			if last_run_result.stdout:
				output("=== STDOUT ===")
				to_out(last_run_result.stdout)
			if last_run_result.stderr:
				output("=== STDERR ===")
				to_out(last_run_result.stderr)
			output("=== END OF LOG ===")


	def post_process(self, params=None, measure_names=None, output_path=None):
		"""
		Runs post processors associated with particular performance measures.

		This method is the place to conduct automatic post-processing
		of core model run results, in particular any post-processing that
		is expensive or that will write new output files into the core model's
		output directory.  The core model run should already have
		been completed using `setup` and `run`.  If the relevant performance
		measures do not require any post-processing to create (i.e. they
		can all be read directly from output files created during the core
		model run itself) then this method does not need to be overloaded
		for a particular core model implementation.

		Args:
			params (dict):
				Dictionary of experiment variables, with keys as variable names
				and values as the experiment settings. Most post-processing
				scripts will not need to know the particular values of the
				inputs (exogenous uncertainties and policy levers), but this
				method receives the experiment input parameters as an argument
				in case one or more of these parameter values needs to be known
				in order to complete the post-processing.  In this demo, the
				params are not needed, and the argument is optional.
			measure_names (List[str]):
				List of measures to be processed.  Normally for the first pass
				of core model run experiments, post-processing will be completed
				for all performance measures.  However, it is possible to use
				this argument to give only a subset of performance measures to
				post-process, which may be desirable if the post-processing
				of some performance measures is expensive.  Additionally, this
				method may also be called on archived model results, allowing
				it to run to generate only a subset of (probably new) performance
				measures based on these archived runs. In this demo, the
				the argument is optional; if not given, all measures will be
				post-processed.
			output_path (str, optional):
				Path to model outputs.  If this is not given (typical for the
				initial run of core model experiments) then the local/default
				model directory is used.  This argument is provided primarily
				to facilitate post-processing archived model runs to make new
				performance measures (i.e. measures that were not in-scope when
				the core model was actually run).

		Raises:
			KeyError:
				If post process is not available for specified measure
		"""
		self.log("RoadTestFileModel POST-PROCESS ...", level=logging.DEBUG)

		if measure_names is None:
			measure_names = set(self.scope.get_measure_names())
		else:
			# Convert the collection of measure_names to a set for easy
			# checking if each target measure is in measure_names
			measure_names = set(measure_names)

			# Check if any measure names not in the scope are given.
			# Raise a KeyError if there are any.
			unknown_measure_names = measure_names - set(self.scope.get_measure_names())
			if unknown_measure_names:
				raise KeyError(unknown_measure_names)

		# Create Outputs directory as needed.
		os.makedirs(
			join_norm(self.local_directory, self.model_path, self.rel_output_path),
			exist_ok=True,
		)

		# These measures are included in the first post-processing block
		block_1 = {
			'value_of_time_savings',
			'present_cost_expansion',
			'cost_of_capacity_expansion',
			'net_benefits',
		}

		if block_1 & measure_names:

			# Do some processing to recover values from output.csv.gz
			df = pd.read_csv(
				join_norm(self.local_directory, self.model_path, 'output.csv.gz'),
				index_col=0,
			)
			repair = pd.isna(df.loc['plain'])
			df.loc['plain', repair] = np.log(df.loc['exp', repair])*1000
			# Write edited output.csv.gz to Outputs directory.
			df.to_csv(
				join_norm(self.local_directory, self.model_path, self.rel_output_path, 'output_1.csv.gz')
			)

		block_2 = {
			'build_travel_time',
			'no_build_travel_time',
			'time_savings',
		}

		if block_2 & measure_names:

			# Copy output.yaml to Outputs directory, no editing needed.
			shutil.copy2(
				join_norm(self.local_directory, self.model_path, 'output.yaml'),
				join_norm(self.local_directory, self.model_path, self.rel_output_path, 'output.yaml'),
			)

		# Log the names of all the files in the local directory
		_logger.debug(f"Files in {self.local_directory}")
		for i,j,k in os.walk(self.local_directory):
			for eachfile in k:
				_logger.debug(join_norm(i,eachfile).replace(self.local_directory, '.'))

		self.log(f"RoadTestFileModel POST-PROCESS complete RUNID-{self.run_id}")


	def archive(self, params, model_results_path=None, experiment_id=None):
		"""
		Copies model outputs to archive location.

		Args:
			params (dict):
				Dictionary of experiment variables
			model_results_path (str, optional):
				The archive path to use.  If not given, a default
				archive path is constructed based on the scope name
				and the experiment_id.
			experiment_id (int, optional):
				The id number for this experiment.  Ignored if the
				`model_results_path` argument is given.

		"""
		if model_results_path is None:
			if experiment_id is None:
				db = getattr(self, 'db', None)
				if db is not None:
					experiment_id = db.get_experiment_id(self.scope.name, params)
			model_results_path = self.get_experiment_archive_path(experiment_id)
		self.log(
			f"RoadTestFileModel ARCHIVE\n"
			f" from: {join_norm(self.local_directory, self.model_path)}\n"
			f"   to: {model_results_path}"
		)
		copy_tree(
			join_norm(self.local_directory, self.model_path),
			model_results_path,
		)

