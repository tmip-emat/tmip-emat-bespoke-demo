
import emat
import emat.examples

def make_colleague_data(filename="road-test-colleague.sqlitedb"):
	s, db, m = emat.examples.road_test(filename)
	experiments = m.design_experiments(design_name='lhs_1', n_samples=100, random_seed=1234)
	m.run_experiments(experiments.iloc[50:])
	return filename

if __name__ == '__main__':
	make_colleague_data()
