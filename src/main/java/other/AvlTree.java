package other;


/**
 * @author bystander
 * @date 2020/10/9
 */
public class AvlTree<E extends Comparable<E>> {

    private Node root;
    private int size;
    public AvlTree() {
        root = null;
        size = 0;
    }

    //获取某一节点的高度
    public int getHeight(Node node) {
        if (node == null) {
            return 0;
        }
        return node.height;
    }

    public int getSize() {
        return size;
    }

    /**
     * 获取某一节点的平衡因子
     *
     * @param node
     * @return
     */
    private int getBalanceFactor(Node node) {
        if (node == null) {
            return 0;
        }
        return getHeight(node.left) - getHeight(node.right);
    }

    /**
     * 判断是否是平衡二叉树
     *
     * @return
     */
    private boolean isBalanced() {
        return isBalanced(root);
    }

    private boolean isBalanced(Node node) {
        if (node == null) {
            return true;
        }
        int balanceFactor = getBalanceFactor(node);

        if (balanceFactor > 1) {
            return false;
        }

        return isBalanced(node.left) && isBalanced(node.right);
    }

    /**
     * 右旋操作
     *
     * @param y
     * @return
     */
    private Node rightRotate(Node y) {
        Node x = y.left;
        Node t3 = x.right;
        x.right = y;
        y.left = t3;

        //更新height
        y.height = Math.max(getHeight(y.left), getHeight(y.right)) + 1;
        x.height = Math.max(getHeight(x.left), getHeight(x.right)) + 1;
        return x;
    }

    private Node leftRotate(Node y) {
        Node x = y.right;
        Node t3 = x.left;

        x.left = y;
        y.right = t3;

        //更新height
        y.height = Math.max(getHeight(y.left), getHeight(y.right)) + 1;
        x.height = Math.max(getHeight(x.left), getHeight(x.right)) + 1;
        return x;
    }

    // 向以node为根的二分搜索树中插入元素(key, value)，递归算法
    // 返回插入新节点后二分搜索树的根
    private Node addNode(Node node, E e) {
        if (node == null) {
            size ++;
            return new Node(e);
        }

        if (e.compareTo(node.e) < 0) {
            node.left = addNode(node.left,e);
        } else if (e.compareTo(node.e) > 0) {
            node.right = addNode(node.right, e);
        }

        //更新height
        node.height = Math.max(getHeight(node.left),getHeight(node.right)) + 1;

        //计算平衡因子
        int balanceFactor = getBalanceFactor(node);

        if (balanceFactor > 1 && getBalanceFactor(node.left) > 0) {
            //右旋
            rightRotate(node);
        }
        if (balanceFactor < -1 && getBalanceFactor(node.right) < 0) {
            //左旋
            leftRotate(node);
        }

        if (balanceFactor > 1 && getBalanceFactor(node.left) < 0) {
            //LR
            node.left = leftRotate(node);
            return rightRotate(node);

        }

        //RL
        if (balanceFactor < -1 && getBalanceFactor(node.right) > 0) {
            node.right = rightRotate(node);
            return leftRotate(node);
        }

        return node;

    }

    //向二分搜索树中添加新的元素
    public void addNode( E e) {
        root = addNode(root, e);
    }


    //删除节点
    private Node removeNode(Node node, E e) {
        if (node == null) {
            return null;
        }

        Node retNode;

        if (e.compareTo(node.e) < 0) {
            node.left = removeNode(node.left, e);
            retNode = node;
        } else if (e.compareTo(node.e) > 0) {
            node.right = removeNode(node.right, e);
            retNode = node;
        }
        else{
            //e.compareTo(node.e)  == 0
            //待删除节点左子树为空的情况
            if (node.left == null) {
                Node rightNode = node.right;
                node.right = null;
                size --;
                retNode = rightNode;
            } else if (node.right == null) {
                Node leftNode = node.left;
                node.left = null;
                size -- ;
                retNode = leftNode;
            }
            else{
                // 待删除节点左右子树均不为空的情况
                // 找到比待删除节点大的最小节点, 即待删除节点右子树的最小节点
                // 用这个节点顶替待删除节点的位置
                Node successor = minumum(node.right);
                successor.right = removeNode(node.right,successor.e);
                successor.left = node.left;

                node.left = node.right = null;

                retNode = successor;
            }


        }

        if(retNode==null)
            return null;
        //维护平衡
        //更新height
        retNode.height = 1+Math.max(getHeight(retNode.left),getHeight(retNode.right));
        //计算平衡因子
        int balanceFactor = getBalanceFactor(retNode);
        if(balanceFactor > 1 && getBalanceFactor(retNode.left)>=0) {
            //右旋LL
            return rightRotate(retNode);
        }
        if(balanceFactor < -1 && getBalanceFactor(retNode.right)<=0) {
            //左旋RR
            return leftRotate(retNode);
        }
        //LR
        if(balanceFactor > 1 && getBalanceFactor(retNode.left) < 0){
            node.left = leftRotate(retNode.left);
            return rightRotate(retNode);
        }
        //RL
        if(balanceFactor < -1 && getBalanceFactor(retNode.right) > 0){
            node.right = rightRotate(retNode.right);
            return leftRotate(retNode);
        }
        return retNode;
    }

    private Node minumum(Node right) {
        return null;
    }


    private class Node {
        public E e;
        public Node left;
        public Node right;
        public int height;

        public Node(E e) {
            this.e = e;
            this.left = null;
            this.right = null;
            this.height = 1;

        }
    }


}
