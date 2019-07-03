
from gensim.scripts.glove2word2vec import glove2word2vec

# wget http://nlp.stanford.edu/data/glove.6B.zip
# unzip glob.6B.zip


def load_model_parameters(model_parameters='glove.6B.50d.txt'):
    """
    transforming model parameters
    """
    glove_input_file = model_parameters
    word2vec_output_file = model_parameters + 'word2vec'
    glove2word2vec(glove_input_file, word2vec_output_file)


if __name__ == '__main__':
    load_model_parameters()
