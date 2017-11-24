import tensorflow as tf
import numpy as np
import argparse
import pickle
import os

from gensim.models import Doc2Vec
from sklearn.utils import shuffle


DOC2VEC_LEN = 300
BATCH_SIZE = 64
ITERATIONS = 10000


class PickleNet:
    def __init__(self):
        self.x = None
        self.y_ = None
        self.layer_0 = None
        self.layer_1 = None
        self.layer_2 = None
        self.y_pred = None
        self.loss = None
        self.train_step = None
        self.summary = None

    @staticmethod
    def _weight_variable(shape, name):
        initial = tf.truncated_normal(shape, stddev=0.1)
        return tf.Variable(initial, name=name)

    @staticmethod
    def _bias_variable(shape, name):
        initial = tf.constant(0.1, shape=shape)
        return tf.Variable(initial, name=name)

    def _create_placeholders(self):
        with tf.name_scope("data"):
            self.x = tf.placeholder(dtype=tf.float32, shape=[None, DOC2VEC_LEN * 2], name='X')
            self.y_ = tf.placeholder(dtype=tf.float32, shape=[None, 1], name='Y_')

    def _create_fully_conn(self, input_tensor, from_n, to_n, layer_name):
        with tf.name_scope(layer_name):
            W_fc = self._weight_variable(shape=[from_n, to_n], name=layer_name + '_W')
            b_fc = self._bias_variable(shape=[to_n], name=layer_name + '_b')
            h_fc = tf.nn.tanh(tf.add(tf.matmul(input_tensor, W_fc), b_fc))
            return h_fc

    def _create_fully_conn_logit(self, input_tensor, from_n, to_n, layer_name):
        with tf.name_scope(layer_name):
            W_fc = self._weight_variable(shape=[from_n, to_n], name=layer_name + '_W')
            b_fc = self._bias_variable(shape=[to_n], name=layer_name + '_b')
            h_fc = tf.add(tf.matmul(input_tensor, W_fc), b_fc)
            return h_fc

    def _create_net(self):
        with tf.name_scope('net'):
            self.layer_0 = self._create_fully_conn(self.x, DOC2VEC_LEN * 2, 512, 'layer_0')
            self.layer_1 = self._create_fully_conn(self.layer_0, 512, 512, 'layer_0')
            self.layer_2 = self._create_fully_conn(self.layer_1, 512, 512, 'layer_0')
            self.y_pred = self._create_fully_conn_logit(self.layer_2, 512, 1, 'layer_0')

    def _create_loss(self):
        with tf.name_scope("loss"):
            self.loss = tf.reduce_mean(tf.losses.huber_loss(labels=self.y_, predictions=self.y_pred))

    def _create_optimizer(self):
        with tf.name_scope("optimizer"):
            self.train_step = tf.train.AdamOptimizer(1e-4).minimize(self.loss)

    def _create_summaries(self):
        with tf.name_scope("summaries"):
            tf.summary.scalar('loss', self.loss)
            self.summary = tf.summary.merge_all()

    def build_graph(self):
        self._create_placeholders()
        self._create_net()
        self._create_loss()
        self._create_optimizer()
        self._create_summaries()


    @staticmethod
    def generate_batch(X, y):
        while True:
            X_shuffled, y_shuffled = shuffle(X, y)

            i = 0

            while i < X_shuffled.shape[0]:
                yield X_shuffled[i: i + BATCH_SIZE], y_shuffled[i: i + BATCH_SIZE]
                i += BATCH_SIZE

    def train(self, X, y):
        with tf.Session() as sess:
            sess.run(tf.global_variables_initializer())
            summary_writer = tf.summary.FileWriter('graphs', sess.graph)

            saver = tf.train.Saver()

            ckpt = tf.train.get_checkpoint_state('checkpoints/checkpoint')
            if ckpt and os.path.isfile(ckpt.model_checkpoint_path):
                saver.restore(sess, ckpt.model_checkpoint_path)

            dataset_iter = self.generate_batch(X, y)

            for i in range(ITERATIONS):
                X_batch, y_batch = next(dataset_iter)
                if i % 100 == 0:
                    loss = sess.run(self.loss, feed_dict={self.x: X_batch, self.y_: y_batch})
                    print('step {}, training loss {}'.format(i, loss))
                _, summary_str = sess.run([self.train_step, self.summary],
                                          feed_dict={self.x: X_batch, self.y_: y_batch})
                summary_writer.add_summary(summary_str, i)
                saver.save(sess, 'checkpoints/pickle_net')


def prepare_dataset(feed_file, doc2vec):
    with open(feed_file, 'rb') as handle:
        cont = pickle.load(handle)

    X = []
    y = []

    for item in cont:
        tag, query, uid, score = item
        if tag == 's':
            vector0 = np.array(doc2vec.infer_vector(query))
        elif tag == 'r':
            vector0 = np.array(doc2vec.docvecs['sent{}'.format(query)])
        vector1 = np.array(doc2vec.docvecs['sent{}'.format(uid)])

        x = np.hstack((vector0, vector1))
        X.append(x)
        y.append(score)

    return np.array(X), np.array(y)


def main(args):
    model = Doc2Vec.load('forward_index.doc2vec')

    net = PickleNet()

    X, y = prepare_dataset('feedback.pickle', model)

    net.train(X, y)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='My purpose is to assess your feedback')

    args = parser.parse_args()

    main(args)
