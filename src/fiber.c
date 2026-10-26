
#define _GNU_SOURCE
#include <assert.h>
#include <unistd.h>
#include <sched.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/mman.h>
#include <stdlib.h> 
#include <string.h> 
#include <pthread.h> 
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
/* Error List*/ 
[[maybe_unused]] constexpr int FIB_ERRFAIL = -1;
/* Enum Definitions */

/* Reservation ID for the Reservation Tickets */
enum Reserv_ID : uint8_t{
	RSV_ATENDING = 0x00   , 
	RSV_WAITING = 0x01 , 
	RSV_DONE = 0x02
};

/* Enum state of FIber state */ 
enum Fib_state_ID : uint8_t {
	FIBST_RUNNING ,
	FIBST_INQUEUE, 
	FIBST_YIELDING,
	FIBST_FINSHED 
};


/* NEEDED TEMPLATE MACROS*/

#define CACHE_LINE 64ULL
#define HD_DESTRUCTIVE_INF_SZ (CACHE_LINE* 2ULL ) /* Due to agressive prefetchers on modern processor the Alignment for destructive infrence size will be set to the cache line * 2 */

/* Value of number of cores are set at compiile time to chnage this define your own */
#define NO_OF_THREADS 4ULL
#define NO_OF_HARVESTERS (NO_OF_THREADS -1ULL)
#define STACK_SIZE_DEFAULT 64ULL * 1024ULL /* 64KB Default configuration */
#define STACK_SIZE (STACK_SIZE_DEFAULT)
#define QUEUE_NO 1024ULL
#define QUEUE_SIZE (QUEUE_NO * 128ULL)
#define RESULT_QUEUE_SZ  (64ULL * QUEUE_SIZE )
#define LOCAL_BUF_MAX_CNT 100ULL 
#define LOCAL_BUF_SZ (LOCAL_BUF_MAX_CNT * 128ULL) 
#define MASK_4KB (0xFFFULL)

/* Struct Defintions */ 

/* this contains the resrvation tickets of threads that want to write to  a queue */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) Reserv_write_tickets{
	_Atomic(uint8_t) m_mask ; /* This holds the state of the reservation tickets */	
	size_t m_position ;
};

/* This represents the reservation tickets of the Harvester hungry to steal */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) Reserv_steal_tickets{
	_Atomic(uint8_t) m_mask ; /* This holds the state of the reservation tickets */	
	size_t m_position ;
};
struct __attribute__((aligned(CACHE_LINE)))scheduler_local{
	size_t id ;
	size_t m_write ; 
	size_t m_steal ;

};
struct mpsc_queue{

	/* Head written to by the scheduler 
	 * NOTE : DUE to the large amoutn of producer this will cause great contention on the head_ atomic value and will sat		urate the buss local copy will be kept but also only the scheduler after collation of result from the producer cores w		ill write to this varaible 
	 */ 
	alignas(HD_DESTRUCTIVE_INF_SZ) _Atomic(uint64_t) m_head_  ;

	/* tail is consume and gooubled up by the consumer*/
	alignas(HD_DESTRUCTIVE_INF_SZ) _Atomic(uint64_t) m_tail_ ;
};

struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ)))Scheduler{
	alignas(HD_DESTRUCTIVE_INF_SZ) struct Reserv_steal_tickets m_steal_tickets [NO_OF_HARVESTERS] ;  
	alignas(HD_DESTRUCTIVE_INF_SZ) struct Reserv_write_tickets m_write_tickets[NO_OF_HARVESTERS];
	// Scheduler Local data 
	struct mpsc_queue m_que_info[NO_OF_HARVESTERS];
	struct scheduler_local m_local_data;	

};
/* This saves calle saved non volatile registers for the x86 Architecture */
struct x86_registers {
	size_t rsp ; 
	size_t rbp ; 
	size_t rbx ; 
	size_t r12 ; 
	size_t r13 ; 
	size_t r14 ; 
	size_t r15 ; 
};

/* This contains the values for being entered into the function 
 * NOTE FOR SPACE CONSTRAINS FUNCTIONS WIL ONLY SUPPORT */ 
struct x86_abi_func_registers{
 	size_t rdi , rsi , rdx , rcx , r8;	
};


struct __attribute__((aligned(CACHE_LINE)))fiber_data{
	void*(*m_func_addr)(void*) ; /* Adrress of function */
	struct x86_registers m_registers ;
	size_t m_position ; 
	uint8_t m_mask ; /* This contains the state od teh function */
};

#define DEFINE_RESULT(TypeName , DataType, Alignment ) \
	struct TypeName{\
		static_assert(sizeof(DataType) <= 64 , "The Data size is greatear than 64 byte alignement " );\
		_Atomic(uint8_t) m_mask ; \
		alignas(_Alignof(DataType)) DataType m_data ;\
	}



/* Harvester contains data of the cores that acts as consumers */
struct Harvester{
	alignas(HD_DESTRUCTIVE_INF_SZ) unsigned char* m_stack ;
	struct mpsc_queue* m_queue_info ;
	void* m_queue;
	void* m_steal_queue ; 
	void* m_write_queue ;
	unsigned char* m_local_copy_buf ;
	uint32_t write_idx ; 
	uint32_t read_idx ; 
	uint32_t rem_idx ;
	uint16_t m_id ;
	void* m_result_queue;
};

/*Fiber */
struct __attribute__((aligned(HD_DESTRUCTIVE_INF_SZ))) fiber {
	struct Scheduler m_scheduler ;
	void* m_queue[NO_OF_HARVESTERS];/* Addrresses of all the queues*/
	void* m_result_queues_ [NO_OF_HARVESTERS]; /* Addresses of Result Queues */ 
};

struct ls_t{/* launch scheduler function parameters */
		struct Scheduler* m_schd;
};


struct lh_t { /* Launch harvester type*/
	uint16_t m_core;
	void* m_addr ;
	void* m_steal_queue ; 
	void* m_write_queue ;
	void* m_result_queue;
	struct mpsc_queue* m_info ;
};
/* Function Declerations*/

void  fiber_init(struct fiber* ); 
int fiber_deinit(struct fiber*) ; 
int fiber_switch(struct fiber_data* , struct fiber_data*) ; 
int fiber_trampoline(struct fiber_data*);/* set up a fiber_data*/

/* schedulers */ 
static void*  launch_scheduler(void*);
int scheduler_deinit(struct Scheduler* );
int scheduler_dst_write(struct Scheduler*);
int scheduler_dst_steals(struct Scheduler*);

static void* launch_harvester(void* );
int harvester_deinit(struct Harvester*);
int harvest_functions(struct Harvester* ); 
static int __hv_fragmentation_internal_check(struct Harvester*); 
/*Function defintions */

__attribute__((noinline , cold)) void crash_fail(){
	fprintf(stderr , "Error intilization ") ; 
	_exit(1) ;
}


__attribute__((noinline , cold)) void crash_fail_cpu(){
	fprintf(stderr , "Failed to lock cpu : sorry  ") ; 
	_exit(1) ;
}
void  fiber_init(struct fiber* fib  ){
	if(!fib){crash_fail();}
	
	// Set up quueus and result queuue memory space 
	for(size_t i = 0 ; i < NO_OF_HARVESTERS ; i++){
		void* ptr ;
		ptr  = mmap(NULL, QUEUE_SIZE, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
		if(ptr == MAP_FAILED){
			crash_fail();
		}
		fib->m_queue[i] = ptr ;
		ptr  = mmap(NULL , RESULT_QUEUE_SZ, PROT_READ| PROT_WRITE , MAP_ANONYMOUS | MAP_PRIVATE , 
				-1 , 0);
		if(ptr == MAP_FAILED ) {crash_fail();}
		fib->m_result_queues_[i] = ptr ;
	}

	// Set up the scheduler 
	pthread_t pid ;
	int ret ;
	pthread_attr_t attr ;
	pthread_attr_t attr2 ;
	if( pthread_attr_init(&attr) < 0 ) {
		crash_fail();
	}
	struct ls_t m_arg ;
	m_arg.m_schd = &(fib->m_scheduler) ;  
	ret =pthread_create(&pid, &attr,&launch_scheduler , (void*) &m_arg);
	if(ret <= -1 ) {
		crash_fail();
	}

	pthread_detach(pid);

	// setting up HArvesters
	struct lh_t m_arg2 ; 
	for(size_t i = 1 ; i <= (size_t)NO_OF_HARVESTERS ; i++){
		if( pthread_attr_init(&attr2) < 0 ) {
		crash_fail();
		}
		pid = 0 ;
		m_arg2.m_core = (uint16_t)i;
		m_arg2.m_addr = &(fib->m_queue[i - 1 ]) ;
		m_arg2.m_result_queue  =(fib->m_result_queues_[i-1]);
		m_arg2.m_steal_queue = (void*) &(fib->m_scheduler.m_steal_tickets[i-1]);
		m_arg2.m_write_queue = (void*) &(fib->m_scheduler.m_write_tickets[i-1]);
		m_arg2.m_info = &(fib->m_scheduler.m_que_info[i-1]);
		ret =pthread_create(&pid , &attr2 , &launch_harvester , (void*)&m_arg2);
		if(ret <0 ) crash_fail() ; 
		pthread_detach(pid);
	}

}

__attribute__((always_inline )) static void __scheduler_local_init(struct scheduler_local* lc , size_t id ){
	lc->id = id; 
	lc->m_steal = 0 ; 
	lc->m_write = 0 ;
}
static void* launch_scheduler(void* arg){
	// lock core 
	int ret ;
	cpu_set_t cpu_set ;
	CPU_ZERO(&cpu_set); 
	
	CPU_SET(0, &cpu_set);
	pthread_t pid = pthread_self();
	// lock core
	ret =  pthread_setaffinity_np(pid , sizeof(cpu_set_t) , &cpu_set);

	if(ret != 0 ){
		crash_fail_cpu();	
	}
	// locak memeory 
	if(!arg) return NULL;
	const struct ls_t* args = (const struct  ls_t* ) arg ;
	memset((args->m_schd->m_steal_tickets),0x00, sizeof(struct Reserv_steal_tickets));
	memset((args->m_schd->m_write_tickets),0x00, sizeof(struct Reserv_write_tickets));
	for(size_t i = 0 ; i < NO_OF_HARVESTERS ; i++){

		atomic_store_explicit(&(args->m_schd->m_que_info[i].m_head_), 0 , memory_order_release) ;
		atomic_store_explicit(&(args->m_schd->m_que_info[i].m_tail_), 0 , memory_order_release) ;
	}
	__scheduler_local_init(&(args->m_schd->m_local_data),0 );
	// start scheduler loop
	while(true){}
	return NULL ;
	
}

static void* launch_harvester(void* arg){
	if(!arg) return NULL ;
	struct Harvester local_harvester ;
	auto args = (struct lh_t*)arg;
	int ret ;
	cpu_set_t cpu_set ;
	CPU_ZERO(&cpu_set); 
	
	CPU_SET(args->m_core, &cpu_set);
	pthread_t pid = pthread_self();
	// lock core
	ret =  pthread_setaffinity_np(pid , sizeof(cpu_set_t) , &cpu_set);

	if(ret != 0 ){
		crash_fail_cpu();	
	}

	/*set up stack */ 
	/* stack default to 64KB*/
	auto ptr = mmap(NULL , STACK_SIZE , PROT_READ| PROT_WRITE , MAP_ANONYMOUS | MAP_PRIVATE , -1 , 0 );
	if(ptr == MAP_FAILED) {
		crash_fail();
	}
	local_harvester.m_stack = ptr ;
	ret = mlock(ptr , STACK_SIZE);
	if(ret == -1){
		crash_fail();
	}
	// lock memery
	ret = mlock(args->m_addr , QUEUE_SIZE);
	if(ret == -1){
		crash_fail();
	}
	// set up local stack 
	
	local_harvester.m_queue = args->m_addr ; 
	local_harvester.m_steal_queue = args->m_steal_queue ; 
	local_harvester.m_write_queue = args->m_write_queue ; 
	local_harvester.m_queue_info = args->m_info ;
	/*Set up local copy buffer */
	ptr = mmap(NULL ,LOCAL_BUF_SZ , PROT_READ| PROT_WRITE , MAP_ANONYMOUS | MAP_PRIVATE , -1 , 0 );
	if(ptr == MAP_FAILED) {
		crash_fail();
	}
	if(ptr == MAP_FAILED){crash_fail();}
	local_harvester.m_local_copy_buf = ptr; 
	
	local_harvester.write_idx = 0 ; 
	local_harvester.read_idx = 0 ; 
	local_harvester.rem_idx = 0 ; 

	local_harvester.m_id = args->m_core ;
	local_harvester.m_result_queue = args->m_result_queue;

	while(true){}	
	return NULL ;
}	
int main(){

	return 0;
}
