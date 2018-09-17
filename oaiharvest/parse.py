import json
import xmltodict
import dask.delayed
from collections import OrderedDict

class arXivParser:
    def __init__(self, output_fpath='./arXiv_metadata_%012d.json', parse_journals=False):
        """
        Parses all xml arXiv metadata files in the chosen directory
        :param dir_path: Directory to parse
        :param parse_journals: Whether to include journal-ref (if they have it) or not
        """
        self._fpath = output_fpath
        self._parse_journals = parse_journals
        self._parsed_dict = dict()
        # Primary and secondary categories dictionaries [src: http://arxitics.com/help/categories]
        self._main_categories = {'cs': 'Computer Science',
                                 'econ': 'Economics',
                                 'eess': 'Electrical Engineering and Systems Science',
                                 'math': 'Mathematics',
                                 'physics': 'Physics (Other)',
                                 'astro-ph': 'Astrophysics',
                                 'cond-mat': 'Condensed Matter',
                                 'gr-qc': 'General Relativity and Quantum Cosmology',
                                 'hep-ex': 'High Energy Physics - Experiment',
                                 'hep-lat': 'High Energy Physics - Lattice',
                                 'hep-ph': 'High Energy Physics - Phenomenology',
                                 'hep-th': 'High Energy Physics - Theory',
                                 'math-ph': 'Mathematical Physics',
                                 'nlin': 'Nonlinear Sciences',
                                 'nucl-ex': 'Nuclear Experiment',
                                 'nucl-th': 'Nuclear Theory',
                                 'quant-ph': 'Quantum Physics',
                                 'q-bio': 'Quantitative Biology',
                                 'q-fin': 'Quantitative Finance',
                                 'stat': 'Statistics'}
        self._sub_categories = {
            'astro-ph':
                {
                    'GA': 'Astrophysics of Galaxies',
                    'CO': 'Cosmology and Nongalactic Astrophysics',
                    'EP': 'Earth and Planetary Astrophysics',
                    'HE': 'High Energy Astrophysical Phenomena',
                    'IM': 'Instrumentation and Methods for Astrophysics',
                    'SR': 'Solar and Stellar Astrophysics'
                },
            'cond-mat':
                {
                    'dis-nn': 'Disordered Systems and Neural Networks',
                    'mtrl-sci': 'Materials Science',
                    'mes-hall': 'Mesoscale and Nanoscale Physics',
                    'other': 'Other Condensed Matter',
                    'quant-gas': 'Quantum Gases',
                    'soft': 'Soft Condensed Matter',
                    'stat-mech': 'Statistical Mechanics',
                    'str-el': 'Strongly Correlated Electrons',
                    'supr-con': 'Superconductivity'
                },
            'nlin':
                {
                    'AO': 'Adaptation and Self-Organizing Systems',
                    'CG': 'Cellular Automata and Lattice Gases',
                    'CD': 'Chaotic Dynamics',
                    'SI': 'Exactly Solvable and Integrable Systems',
                    'PS': 'Pattern Formation and Solitons'
                },
            'physics':
                {
                    'acc-ph': 'Accelerator Physics',
                    'app-ph': 'Applied Physics',
                    'ao-ph': 'Atmospheric and Oceanic Physics',
                    'atom-ph': 'Atomic Physics',
                    'atm-clus': 'Atomic and Molecular Clusters',
                    'bio-ph': 'Biological Physics',
                    'chem-ph': 'Chemical Physics',
                    'class-ph': 'Classical Physics',
                    'comp-ph': 'Computational Physics',
                    'data-an': 'Data Analysis, Statistics and Probability',
                    'flu-dyn': 'Fluid Dynamics',
                    'gen-ph': 'General Physics',
                    'geo-ph': 'Geophysics',
                    'hist-ph': 'History and Philosophy of Physics',
                    'ins-det': 'Instrumentation and Detectors',
                    'med-ph': 'Medical Physics',
                    'optics': 'Optics',
                    'ed-ph': 'Physics Education',
                    'soc-ph': 'Physics and Society',
                    'plasm-ph': 'Plasma Physics',
                    'pop-ph': 'Popular Physics',
                    'space-ph': 'Space Physics'
                },
            'math':
                {
                    'AG': 'Algebraic Geometry',
                    'AT': 'Algebraic Topology',
                    'AP': 'Analysis of PDEs',
                    'CT': 'Category Theory',
                    'CA': 'Classical Analysis and ODEs',
                    'CO': 'Combinatorics',
                    'AC': 'Commutative Algebra',
                    'CV': 'Complex Variables',
                    'DG': 'Differential Geometry',
                    'DS': 'Dynamical Systems',
                    'FA': 'Functional Analysis',
                    'GM': 'General Mathematics',
                    'GN': 'General Topology',
                    'GT': 'Geometric Topology',
                    'GR': 'Group Theory',
                    'HO': 'History and Overview',
                    'IT': 'Information Theory',
                    'KT': 'K-Theory and Homology',
                    'LO': 'Logic',
                    'MP': 'Mathematical Physics',
                    'MG': 'Metric Geometry',
                    'NT': 'Number Theory',
                    'NA': 'Numerical Analysis',
                    'OA': 'Operator Algebras',
                    'OC': 'Optimization and Control',
                    'PR': 'Probability',
                    'QA': 'Quantum Algebra',
                    'RT': 'Representation Theory',
                    'RA': 'Rings and Algebras',
                    'SP': 'Spectral Theory',
                    'ST': 'Statistics Theory',
                    'SG': 'Symplectic Geometry'
                },
            'cs':
                {
                    'AI': 'Artificial Intelligence',
                    'CL': 'Computation and Language',
                    'CC': 'Computational Complexity',
                    'CE': 'Computational Engineering, Finance, and Science',
                    'CG': 'Computational Geometry',
                    'GT': 'Computer Science and Game Theory',
                    'CV': 'Computer Vision and Pattern Recognition',
                    'CY': 'Computers and Society',
                    'CR': 'Cryptography and Security',
                    'DS': 'Data Structures and Algorithms',
                    'DB': 'Databases',
                    'DL': 'Digital Libraries',
                    'DM': 'Discrete Mathematics',
                    'DC': 'Distributed, Parallel, and Cluster Computing',
                    'ET': 'Emerging Technologies',
                    'FL': 'Formal Languages and Automata Theory',
                    'GL': 'General Literature',
                    'GR': 'Graphics',
                    'AR': 'Hardware Architecture',
                    'HC': 'Human-Computer Interaction',
                    'IR': 'Information Retrieval',
                    'IT': 'Information Theory',
                    'LG': 'Learning',
                    'LO': 'Logic in Computer Science',
                    'MS': 'Mathematical Software',
                    'MA': 'Multiagent Systems',
                    'MM': 'Multimedia',
                    'NI': 'Networking and Internet Architecture',
                    'NE': 'Neural and Evolutionary Computing',
                    'NA': 'Numerical Analysis',
                    'OS': 'Operating Systems',
                    'OH': 'Other Computer Science',
                    'PF': 'Performance',
                    'PL': 'Programming Languages',
                    'RO': 'Robotics',
                    'SI': 'Social and Information Networks',
                    'SE': 'Software Engineering',
                    'SD': 'Sound',
                    'SC': 'Symbolic Computation',
                    'SY': 'Systems and Control'
                },
            'q-bio':
                {
                    'BM': 'Biomolecules',
                    'GN': 'Genomics',
                    'MN': 'Molecular Networks',
                    'SC': 'Subcellular Processes',
                    'CB': 'Cell Behavior',
                    'NC': 'Neurons and Cognition',
                    'TO': 'Tissues and Organs',
                    'PE': 'Populations and Evolution',
                    'QM': 'Quantitative Methods',
                    'OT': 'Other'
                },
            'q-fin':
                {
                    'PR': 'Pricing of Securities',
                    'RM': 'Risk Management',
                    'PM': 'Portfolio Management',
                    'TR': 'Trading and Microstructure',
                    'MF': 'Mathematical Finance',
                    'CP': 'Computational Finance',
                    'ST': 'Statistical Finance',
                    'GN': 'General Finance',
                    'EC': 'Economics'
                },
            'stat':
                {
                    'AP': 'Applications',
                    'CO': 'Computation',
                    'ML': 'Machine Learning',
                    'ME': 'Methodology',
                    'OT': 'Other Statistics',
                    'TH': 'Theory'
                }
        }

    def _parse_doc(self, doc, journal=False):
        """
        Parses a given xmltodict document
        :param doc: dictionary of xml objects
        :param journal: whether or not to include journal-ref as an output key; Default=False
        :return: the parsed dictionary including title, authors, (journal-ref; optional), and categories
        """
        # High level metadata that requires no preprocessing
        parsed_one = dict()
        parsed_one['title'] = doc['arXiv']['title']
        parsed_one['authors'] = doc['arXiv']['authors']
        if journal:
            parsed_one['journal-ref'] = doc['arXiv']['journal-ref']

        parsed_one['abstract'] = doc['arXiv']['abstract']

        # The raw categories is a string of categories split by spaces
        cats = list()
        for category_group in doc['arXiv']['categories'].split(' '):
            cats.append(category_group.split('.'))  # Primary and secondary categories are separated by periods
        # Categories must be preprocessed into an OrderedDict
        parsed_one['categories'] = OrderedDict()
        cat_iter = 0
        for cat in cats:  # For each
            if cat[0] in self._main_categories.keys():  # Make sure it exists in our main categories dict, then append
                parsed_one['categories'][f'category-{cat_iter}'] = {
                    'main_category': self._main_categories[cat[0]]
                }
                # Make sure it exists in our sub categories dict and it has a sub-category, then append
                if cat[0] in self._sub_categories.keys() and len(cat) > 1:
                    parsed_one['categories'][f'category-{cat_iter}']['sub_category'] = self._sub_categories[cat[0]][
                        cat[1]]
                cat_iter += 1

        return parsed_one

    def _parse_one(self, record):
        """
        A dask delayed function which parses an individual xml file by calling _parse_doc
        :param fpath: file path to the xml file
        :return: Either the output of _parse_doc, or None if neither if condition is satisfied
        """
        doc = xmltodict.parse(record)

        # Filter by journal articles + conference proceedings papers
        if self._parse_journals and 'journal-ref' in doc['arXiv'].keys():  # Either parse journals
            return self._parse_doc(doc, journal=True)
        elif not self._parse_journals:  # Or ignore journal-ref as a key
            return self._parse_doc(doc, journal=False)

        return None

    def parse(self, harvested_list):
        """
        A for loop, which fills a parsed_dict with dask.delayed objects by calling _parse_one on each file
        :return: The dictionary, computed in parallel using dask
        """
        parsed = [dict()]
        cur_file = 0
        for i, record in enumerate(harvested_list):
            if i % 10000 == 0 and i > 0:
                parsed[cur_file] = {k: v for k, v in parsed[cur_file].items() if v is not None}
                parsed.append(dict())
                cur_file += 1
            parsed[cur_file][i] = self._parse_one(record)

        parsed_dicts = dask.compute(parsed)[0]
        self._parsed_dict = [{k: v for k, v in parsed_dict.items() if v is not None} for parsed_dict in parsed_dicts]

    def save(self, fpath=None):
        """
        Writes the computed dictionary to file
        :param fpath: file path to write the json to
        """
        if fpath is None:
            fpath = self._fpath
        if self._parsed_dict:  # Makes sure that parse has been called before trying to save
            for i, parsed_dict in enumerate(self._parsed_dict):
                with open(fpath % i, 'w') as f:
                    json.dump(parsed_dict, f)
        else:
            raise ValueError('The dictionary is empty. Try calling parse() before save().')