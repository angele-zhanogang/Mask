package com.angle;

public class test {
    public static void main(String[] args) {
        ListNode a = new ListNode(2);
        ListNode b = new ListNode(3, a);
        ListNode c = new ListNode(3, b);
        if(c.next!=null){
            
        }
    }


   static class ListNode {
       int val;
       ListNode next;
       ListNode() {
       }
       ListNode(int val) {
           this.val = val;
       }
       ListNode(int val, ListNode next) {
           this.val = val;
           this.next = next;
       }
   }
}
