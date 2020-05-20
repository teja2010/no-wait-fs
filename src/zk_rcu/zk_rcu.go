package zk_rcu

import (
	"errors"
)

// the main recipe for a RCU sync on zookeeper
// needs to be filled in.

type Zk_RCU_res interface {
	Zk_rcu_read_lock() error
	Zk_rcu_read_unlock() error
	Zk_rcu_assign_pointer() error
	Zk_rcu_dereference() error
}

type rcu_data struct {
	resource_name string // name of the resource protected by RCU
}

func Create_RCU_resource(resource_name string) (Zk_RCU_res, error) {
	r := new(rcu_data)
	return r, nil
}

func (r *rcu_data) Zk_rcu_read_lock() error {
	return errors.New("yet to implement")
}
func (r *rcu_data) Zk_rcu_read_unlock() error {
	return errors.New("yet to implement")
}
func (r *rcu_data) Zk_rcu_assign_pointer() error {
	return errors.New("yet to implement")
}
func (r *rcu_data) Zk_rcu_dereference() error {
	return errors.New("yet to implement")
}



