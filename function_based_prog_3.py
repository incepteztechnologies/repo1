#Day 12
#FUNCTION BASED PROGRAMMING (Python is a pure FBP Language)
#What is a FBP - If we are giving high priority for functions and use functions in most of our programming rather than writing the
# functionalities again and again for reducing the complexity. Simply say, FBP will help us convert our prog lang to
# high level (10 lines of code) rather than low level (boiler plate coding 100 lines of code)

#What is Functions/Method in FBP -
#A function/method is a collection of statements that perform a certain task and no output will be returned as a result.
#Function/Method is a collection of statements/code/lines of program for performing a common functionality and output is produced as a result
#Functions are used to put some common and repeated task into a single name, so
#instead of writing the same code again and again for different inputs, we can simply call the (name) function.

#FBP – Characteristics
#Functions are first class citizens - Functions are given equal priority as like varibles
var1=10
var1=lambda arg:arg*arg
var1(5)
#Functions are not time bounded - We can call functions any number of times
a=10
if a==10:
 a=20

def x():
  a=10
  if a==10:
    print(a)
    a=20
    print(a)
x()

#Functions are composable - to achive complex output, we don't have to create a complex function rather we can call/compose simple functions
compname="inceptEz"
compname.lower().count("e")

#Functions are simple to create/manage/call

#Why we need to create Functions or why we need FBP -
#1. Functions can be created once and reused multiple times
#2. Functions are used to call/run in a parallel (I have 1000 rows, running a function in 10 parallel threads with each thread handles 100 rows)
# and in concurrent fashion (I am calling a single function at the sametime from 10 applications/programs)
#3. Functions help us reduce the LOC by reusing and by composing
#4. Functions are used for creating Reusable Frameworks (Masking FW, DMA FW, DQ FW etc.,),
# Generic Frameworks (Spark/Airflow) or custom functionalities (udf, custom functions, Cloud functions) or centralized functionalities (If we change the function
# in one place, that affects the entire consuming applications of the given functions)

#All about Functions or Methods in FBP
###################################################
#1. How to Create a function and call a function (High level)
#A. Minimal/least way of creating a method
#We need to have a def keyword (definition of a function)
#followed by the function name , followed by : (completion of the definition)
#followed by the body of the program (minimum a line of code is needed)
def func_name():
    print("any one line of code")
func_name()

#2. Regular/Optimal way of creating a method
#We need to have a def keyword (definition of a function) - Mandatory
#followed by the function name - Mandatory
#followed by argument(s) - Optional,
# followed with datatype mentioned (to help us understand the datatype signature of the method)- Optional
# mentioned inside the brackets() - Mandatory
#followed by : (completion of the definition) - Mandatory
#followed by : some description in ''' , to display the methods description when hover- Optional
#followed by the body of the program (multiple lines of code - core logics + std output + return) - Mandatory (atleast a single line)
#followed by return keyword - where as return help us send back result to the calling environment - Optional
def sqrt(arg1:int):
    '''Method/Function to calculate square root'''
    square_root=arg1*arg1#core logic
    print(f"square root of {arg1} is {square_root}")#print the result for an understanding purpose
    return square_root#return the logic applied result to the calling environment

#3. What are the possible way of creating functions/methods
def met_with_no_arg_no_return():#I wan't to perform some common functionality alone, no need of results eg. I wan't to clean the temp path
    sqrt_val=5*5

def met_with_no_arg_return():#I wan't to perform some common functionality and needed the result of it, eg. calculate the space in file system
    sqrt_val=5*5
    return sqrt_val

def met_with_arg_no_return(arg): #I wan't to perform some specific functionality, no need the of results, eg. I wan't to clean the path which I wanted to mention
    sqrt_val=arg*arg

def met_with_arg_return(arg):#I wan't to perform some specific functionality and I needed the result of it, eg. calculate the space in the given file system and get the result of it returned
    sqrt_val=arg*arg
    return sqrt_val

#4. How do we call the Methods:
#How to just print the output of a function
met_with_no_arg_no_return()
print(met_with_no_arg_no_return())#output is going to be None

#How to store the output of a method and use that output further
return_val=met_with_no_arg_return()#output is going to be 25
print(return_val)

#How to pass an argument to the function using a local value or hardcoded value
x=10
met_with_arg_no_return(x)#Call the method with argument value passed
met_with_arg_no_return(10)#Call the method with argument value passed
#met_with_arg_no_return()#Call the method with one argument value passed, if not it will fail

#completed the Swiggy usecase -> keep this program inside the function and call it as a function
company_fixed_value_march={"max_off":100,"off_pct":10,"min_puchase":500}
#def swiggy_dynamic_pricing_method(cart_price,company_fixed_value_march):
#    "paste your entire code created"
cart_price=1600
#Balachandar's Code
def swiggy_dynamic_pricing_method(cart_price:int, company_fixed_value_march):
    '''Swiggy Usecase for calculating the dynamic pricing of the product'''
    try:
        min_pur=int(company_fixed_value_march["min_puchase"])
        if cart_price <= min_pur:
            print(f'No Offer is applied, cart price should be greater than {min_pur}')
            #print(f'Final Amount to be paid ', cart_price)
            return cart_price
        else:
            off_percent = cart_price * (float(company_fixed_value_march["off_pct"] / 100))
            max_of = int(company_fixed_value_march["max_off"])
            if off_percent > max_of:
                print(f'Maximum Offer {max_of} is applied')
                fin_amt=cart_price - max_of
                #print(f'Final Amount to be paid {fin_amt}')
                return fin_amt
            else:
                applied_off=round(off_percent)
                print(f'Applied offer is {applied_off}')
                fin_amt=cart_price - applied_off
                #print(f'Final Amount to be paid {fin_amt}')
                return fin_amt
    except Exception as err_msg:
        print(f"Error applying offer to the Cart price"
              f"is : {err_msg}")

company_fixed_value_march={"max_off":100,"off_pct":10,"min_puchase":500}

final_price=swiggy_dynamic_pricing_method(300,company_fixed_value_march)
print(f'Final Amount to be paid {final_price}')

def taxation(final_prc:int):
    tax_final_price=final_prc+(final_prc*.18)
    return  tax_final_price
tax_applied_final_price=taxation(final_price)
print(f'Final tax applied Amount to be paid by the customer {tax_applied_final_price}')

#5. How to call the methods using different Argument types: Important part
##############################################################################
#Different type of methods/functions arguments are there -
# positional arg func, named arg func, default arg func, arbitrary arg func, keyword arbitrary argument function
#Lets take an example of calculating bonus and incentive for our employees of a dept
def bonus_calc(sal,bonus,incentive):
    bon_sal=sal+(sal*(bonus/100))
    bon_sal_inc=bon_sal+incentive
    print(f"Bonus & incentive applied sal is {bon_sal_inc}")
    return bon_sal_inc
#A. Positional arguments methods
total_sal=bonus_calc(10000,.5,1000)#calling the method by passing the arguments in the respective postions
print(round(total_sal))

#B. Named arguments methods
#calling the named arguments methods by passing the arguments assigning with the respective names in any order/position
total_sal=bonus_calc(incentive=1000,sal=10000,bonus=.5)
print(round(total_sal))
#total_sal=bonus_calc(sal=10000,bonus=.5)

#c. Default arguments methods -
# Default arg func/met will help us define the funct with some default args, if the given arg is not passed by the calling environment,
# then default value will be used
def bonus_calc(sal,bonus,incentive=1000):
    bon_sal=sal+(sal*(bonus/100))
    bon_sal_inc=bon_sal+incentive
    print(f"Bonus & incentive applied sal is {bon_sal_inc}")
    return bon_sal_inc
total_sal=bonus_calc(10000,.5,2000)#positional arguments, the value 2000 will be considered
print(round(total_sal))
total_sal=bonus_calc(sal=10000,bonus=.5,incentive=2000)#named arguments, the value 2000 will be considered
print(round(total_sal))
total_sal=bonus_calc(10000,.5)#positional arguments + default arguments, the default value 1000 will be considered
print(round(total_sal))
total_sal=bonus_calc(sal=10000,bonus=.5)#named arguments + default arguments, the default value 1000 will be considered
print(round(total_sal))

#Day 13
#d. Arbitrary (any numbers) Argument Method/Function - Accepts the argument as tuple with the notation of *argument_name
# If we are not sure about the number of arguments that we are going to pass to this method, but we use the same order (position) of passing the arguments
#sal,bonus,incentive=1000
print("d.Arbitrary Argument Function/Method")
def bonus_calc(*args):
    #By adding * before the input argument name, we can create arbitrary arg methods to pass any number of arguments in a single variable by using positional notation
    #in a form of tuple
    #print(args)
    #print(type(args))
    len_of_args=len(args)
    print(f"length={len(args)}")
    if len_of_args==1:
        sal=args[0]
        fin_sal = sal
    elif len_of_args==2:
        sal = args[0]
        bon = args[1]
        bon_sal = sal + (sal * (bon / 100))
        fin_sal = bon_sal
    elif len_of_args==3:
        sal = args[0]
        bon = args[1]
        incen=args[2]
        bon_sal = sal + (sal * (bon / 100))
        fin_sal = bon_sal + incen
    elif len_of_args==4:
        sal = args[0]
        bon = args[1]
        incen=args[2]
        special=args[3]
        bon_sal = sal + (sal * (bon / 100))
        fin_sal = bon_sal + incen+special
    print(f"Bonus & incentive applied sal is {round(fin_sal)}")
    return round(fin_sal)
bonus_calc(10000)
bonus_calc(10000,.5)
bonus_calc(10000,.5,1000)
bonus_calc(10000,.5,1000,1000)

    #bon_sal=sal+(sal*(bonus/100))
    #bon_sal_inc=bon_sal+incentive
    #print(f"Bonus & incentive applied sal is {bon_sal_inc}")
    #return bon_sal_inc

#e. Keyword Arbitrary Argument Method/Function - Accepts the argument as  with the notation of **argument_name
# If we are not sure about the number of arguments that we are going to pass to this method and
#the order of the argument we are going to pass is unknown (Named arguments) , we can use keyword arbitrary arg method
print("e. Keyword-Arbitrary Argument Function/Method")
def bonus_calc(**kwargs):
    #By adding ** before the input argument name, we can create arbitrary keyword arg methods to pass any number of arguments in a single variable in any order or using naming notation
    #in a form of Dict
    print(kwargs)
    print(type(kwargs))
    len_of_args=len(kwargs)
    print(f"length={len(kwargs)}")
    if len_of_args==1:
        sal=kwargs['sal']
        fin_sal = sal
        print(f"sal is {round(fin_sal)}")
    elif len_of_args==2:
        sal = kwargs['sal']
        if list(kwargs.keys()).count('bon'):
            bon = kwargs['bon']
            fin_sal = sal + (sal * (bon / 100))
            print(f"Bonus applied sal is {round(fin_sal)}")
        elif list(kwargs.keys()).count('incen'):
            inc = kwargs['incen']
            fin_sal = sal + inc
            print(f"Incentive applied sal is {round(fin_sal)}")
    elif len_of_args==3:
        sal = kwargs['sal']
        bon = kwargs['bon']
        incen=kwargs['incen']
        bon_sal = sal + (sal * (bon / 100))
        fin_sal = bon_sal + incen
        print(f"Bonus & incentive applied sal is {round(fin_sal)}")
    return round(fin_sal)
bonus_calc(sal=10000)
bonus_calc(sal=10000,bon=.5)
#If we are not going to get bon as an argument, rather we are going to incen
bonus_calc(sal=10000,incen=1000)#here the len will return 2
bonus_calc(sal=10000,bon=.5,incen=1000)
#bonus_calc(10000,.5,1000,1000)

#Usecase: Create a method should take 3 arguments, firstname, lastname, domain to generate mail id
#generate_mail('mohamed','irfan','@inceptez.com') -> mohamedirfan@inceptez.com (if the 3rd is not passed, then return mohamedirfan@gmail.com
#I need to call the above method using all the 3 methodologies we are learning positional, named, default
#I need to call the above method using additional 2 methodologies we are learning arbitrary and keyword arg functions with default arg function -
# I may pass fullname alone or i may pass fname,lname,@inceptez.com in the same order or i may pass only fname,lname in any order def mail(**kwargs,dom='@inceptez.com'):

#6. Special Types of Methods/Functions in Python -
# Higher Order Function, Anonymous/lambda, Higher order functions-Hierarchical/Nested/currying/partial, Closures, recursive functions, (instance, class,static methods)
print("**********Special Types of Methods/Functions in Python - Important (interview purpose)***************")
#a. Higher Order Functions - Top priority
print("a. Higher Order Functions")
print("Benifits of HOF1 -> 1. We can use the functions seperately or along with other functions eg. fullname and mailid functions, "
      "2. Rather than rewriting the code in a given function we can reuse it by passing the function as an argument eg. calc programs")
#1. Type1 Higher order function says - If a function takes another function(s) as an input argument(s) then it is a HOF1
#hof1 - how to understand what is hof in a simple way...
def met1():#HOF-?No - if this func take an input of another funct or returns another funct
    return "my name is method1 "
print(met1())

res=met1()
print(res)
def met2(res):#HOF-?No - if this func take an input of another funct or returns another funct
    return "my name is method2 "+"value="+res
print(met2(res))

def met2(fun):#HOF-?Yes - if this func take an input of another funct or returns another funct
    return "my name is method2 "+"method calling="+fun()
print(met2(met1))

#Reallife example is...
def sal_calc(work_day):#HOF? No, bcz no input or output functions
 work_days=work_day
 if (work_days==30):
  return 10000
 elif work_days>30:
  return 11000
 else:
  return 9000

def bonus_calc(func1,bonus,incentive,workday=31):##HOF? Yes, bcz no input or output functions,
    # This bonus_calc is taking sal_calc as an argument func1, hence bonus_calc is a HOF
    sal=func1(workday)
    bon_sal=sal+(sal*(bonus/100))
    bon_sal_inc=bon_sal+incentive
    print(f"Bonus & incentive applied sal is {round(bon_sal_inc)}")
    return round(bon_sal_inc)
bonus_calc(sal_calc,.10,incentive=1000,workday=28)#HOF+Positional arg funct+named arg funct+default arg funct

#Day 14
#2. Type2 Higher order function says -  If a given func/method returns another func/method then it is a Higher order function - HOF2
#Other names for the HOF type2 - Hierarchical functions/Nested Functions/Partial Functions/Currying Functions
#hof2 - how to understand what is hof2 in a simple way...
#If a fun/met returns another fun/met, then it is hof2
def met1():
    return "My name is simple Method1"

def met1_hof2():#HOF?Yes #this fun/met returns another fun/met, hence it is hof2
    def met2():
        return "some value"
    return met2

fun1=met1_hof2()#calling met1_hof2() to get met2, storing met2 in fun1
fun1()#calling met2
print(met1_hof2()())#return met2 (another func)

#Why HOF2 is otherwise called as - Hierarchical+Nested Functions/Partial Functions/Currying Functions
def met1_hof2():#HOF?Yes #this fun/met returns another fun/met, hence it is hof2
    val="curry"
    def met2():
        return val+" salt"
    return met2

#mother cooking veg curry, don't have salt, get the salt, partially cook until u get the salt
met1_partial_curry_food=met1_hof2()#met2 is returned to me
print("partial food",met1_partial_curry_food)#met2 is returned to me
print("final food - ",met1_partial_curry_food())

#Reallife example of HOF Type2
#Create a calculator simply
def calc(type,arg1,arg2):
    if type=='a':
        return arg1+arg2
    elif type=='s':
        return arg1 - arg2
    elif type=='m':
        return arg1 * arg2
    else:
        return arg1/arg2
print(calc('a',10,20))
print(calc('m',10,20))
print(calc('d',10,20))

#Create a calculator with HOF1 option
def add(x,y):
    return x+y
def sub(x,y):
    return x-y
def mul(x,y):
    return x*y
def div(x,y):
    return x/y
def calc(fun1,type,arg1,arg2):
    if type=='a':
        return fun1(arg1,arg2)
    elif type=='s':
        return fun1(arg1,arg2)
    elif type=='m':
        return fun1(arg1,arg2)
    else:
        return fun1(arg1,arg2)

print(calc(add,'s',20,30))
print(calc(sub,'s',20,30))
print(calc(mul,'m',20,30))

#Create a Calculator program using HOF2
#Create a Calculator program using HOF2
def calc(type):
    if type=='a':
        def add(x, y):
            return x + y
        return add
    elif type=='s':
        def sub(x, y):
            return x - y
        return sub
    elif type=='m':
        def mul(x, y):
            return x * y
        return mul
    else:
        def div(x, y):
            return x / y
        return div
add_fun1=calc('a')#this will return the add function to me
print(add_fun1(10,50))
mul_fun2=calc('m')#this will return the add function to me
print(mul_fun2(10,50))

#Usecase6 - FBP (HOF both types):
#Simulate the mailid creation function using HOF1 and HOF2
#Example1: HOF1: create one function that returns fullname(fname,lname)->fullname
#Create another function to return mailid of the fullname
#Benifit is, we can call the fullname function seperately or call mailid function passing fullname function to get mailid
#Chanakya's code
def full_name(fname,lname):
    return fname+lname
def mail_id(full_name):
    mail='@inceptez.com'#closure
    def info(fname,lname):
        return full_name(fname,lname)+mail
    return info#HOF2
print(full_name('chanakya','aspirant'))
my_info = mail_id(full_name)
my_info('chanakya','aspirant')

#Usha's Code
#mailid function - HOF Type1
def name(fname,lname):
   return fname+lname

#print(name('usha','gunalan'))

def mailid(name_func,domain="@gmail.com"):#HOF1
    email_name=name_func('usha','gunalan')
    print(email_name+domain)

mailid(name,domain='@yahoo.com')
mailid(name)

#mailid function - HOF Type2
def emailid():
    domain = '@gmail.com'#closure
    def mail(fname,lname):
        return fname+lname+domain
    return mail#HOF2

print(emailid()('diya','joseph'))
#print(emailid()('diya','joseph'))

#Day 15
#Example2: HOF1: Convert the simple calculator function into HOF1
#HOF2: Another scenario -
#Convert the HOF2 calculator program to Scientific calculator program?
#scientific calculator
def scientific_calc(calctype,type):
    if calctype=='Normal':
        if type=='a':
            def add(x, y):
                return x + y
            return add
        elif type=='s':
            def sub(x, y):
                return x - y
            return sub
        elif type=='m':
            def mul(x, y):
                return x * y
            return mul
        else:
            def div(x, y):
                return x / y
            return div
    elif calctype=='Scientific':
        import math
        if type=='sin':
            def fun_sin(x):
                return math.sin(x)
            return fun_sin

sin_fun1=scientific_calc('Scientific','sin')
print(sin_fun1(90))

print("b. Closures Functions")
print("If a function is impacted or output is depending of the external values, then it is closure")
#Simple function
def sal_hike(sal,hike):
    fixed_yearend_bonus = 10000
    total_sal=sal+hike+fixed_yearend_bonus
    return total_sal
sal_hike(20000,1000)#Closure Function

#If a function is impacted or output is depending of the external values, then it is closure
fixed_yearend_bonus=10000#external global variable
def sal_hike(sal,hike):
    total_sal=sal+hike+fixed_yearend_bonus
    return total_sal
print("closure function impacted with fixed_yearend_bonus",sal_hike(20000,1000))#Closure Function
print("closure function impacted with fixed_yearend_bonus",sal_hike(30000,2000))#Closure Function

#Tell me what types of special functions we have here...
#The below function is a HOF2 + the bonus function is a closure
def sal_hike(sal,hike):
    fixed_yearend_bonus = 10000  # external global variable
    sal_hike = sal + hike
    def bonus(bon):
        final_sal=sal_hike+bon+fixed_yearend_bonus#closure
        return final_sal
    return bonus #HOF2

fun1=sal_hike(10000,1000)
print(fun1(5000))

print("c. Lambda Function - Anonymous Functions/Function Literal/Value Function")
print("If a funtion is required to be created anonymously and not in a pre defined way, then we can create that function as lambda function")
#Methods created with def keyword will be mostly used across different modules of our application,
# whereas the lambda function will be created and used then and there, not across the application modules
def bon(sal,bon):#Defined method, which can be used across the modules of our application
    return sal+(sal*(bon/100))
print("defined method ",bon(10000,10))
bonus=10
sal_lst=[10000,20000,30000]
for sal in sal_lst:
    print(round(bon(sal, bonus)))

#If the bonus calculation is needed (anonymously) only within this module, we are not going to use anywhere else as a common method?
#then create this method as a lambda function, not as a defined method
bon=10
lam_bon=lambda sal:round(sal+(sal*(bon/100))) #value function or function literally
#lam_bon=def function_name
#lambda sal=(arguments) passed to the def function
#:round(sal+(sal*(bon/100)))=business logid body of the def function
print("lambda function",lam_bon(10000))

#If I have list of salary to be applied on the above lambda function? We have to loop through
sal_lst=[10000,20000,30000]
final_sal_lst=list([])
#final_sal_lst=[11000,22000,33000]
#We can iterate on the list of values and pass to the lambda function using traditional for loop (costly effort and not suggested)
for sal in sal_lst:
    print(round(lam_bon(sal)))
    final_sal_lst.append(lam_bon(sal))
print("for loop",final_sal_lst)
#or
#We can iterate on the list of values and pass to the lambda function using FBP functions (simplified preferred apprach)
final_sal_lst1=list(map(lam_bon,sal_lst))
print("map hof function",final_sal_lst1)
####Upto here if you are clear, we are good..######

#Calculate the variable bonus applied to the respective salary of the given list
#You can feel below one like we are creating arbitrary args lambda function
sal_lst=[(10000,10),(20000,15),(30000,10)]
lam_bon=lambda sal_bon:round(sal_bon[0]+(sal_bon[0]*(sal_bon[1]/100)))
final_sal_lst1=list(map(lam_bon,sal_lst))
print("map hof function",final_sal_lst1)

#Can you rewrite the above program to work with list of dict? You can feel like we are creating kwargs lambda function
sal_dict=[{"sal":10000,"bon":10},(20000,15),(30000,10)]

#Other features of using FBP and with lambda (broader benifits)
sal_lst=[10000,20000,30000,15000,25000]
#Calculate the total sum of sal using lambda functionality?
lam_sum_sal=lambda mind,finger:mind+finger
import functools
print(functools.reduce(lam_sum_sal,sal_lst))#this will also run a for loop and in every iteration it will take first elem and the second elem and pass to the lambda function
#Calculate the max sal using lambda functionality?
lam_max_sal=lambda a,b:a if a>b else b
print(functools.reduce(lam_max_sal,sal_lst))

print("d. Recursive Function - Tail recursion, iterative function")
print("Recursive function is a function that calls himself, hence i can do repeated activities")
#I want to calculate compound interest/SIP/FD - I am investing 10000 after 1 year I am going to get a return of some amount depends on the interest or market growth?
#Jan - depositing 10000 with roi 10% -> feb it will become (10000+1000)=11000 -> mar it will become (11000+1100) = 12100 ->
# apr it will become (12100+1210) = 13310 (maturity)
roi=10
def compound_interest(amt,tenure):
    print("function call")
    if tenure==0:
        print("No more tenure and reached maturity")
        return amt
    else:
        print("actual logic is running",amt)
        amt=amt+(amt*(roi/100))
        tenure=tenure-1
        return compound_interest(amt, tenure)

print(round(compound_interest(10000,12)))

#Some of the interview questions asked in python or any other programming languages?
#Check whether the given number is prime?
#Calculate the factorial of n value?
#factorial of 4 is 4*3*2*1
#Create fibboneci series? Reallife we use in our Agile program (module is assigned to us by agile coach/scrum master) story points will be created in jira
#story points will be in fibboneci series
#0 1 1 2 3 5 8 13 21
#Calculate the given value is even or odd?
#Create some pattern of astrics?
#check whether the given text is palindrome?

def factorial(val):
    if val==1:
        return 1
    else:
        return val*factorial(val-1)

print(factorial(5))
#5

#0 1 1 2 3 5 8 13 21
def fibo(val):
    if val==0:
        return 0
    if val==1:
        return 1
    else:
        return fibo(val-1)+fibo(val-2)
for i in range(0,10):
    print(fibo(i))

#not a suggested apprach
#from learn.python.scratch_learn import lam_func
#print(lam_func(10,20))

#Day 16 - Conclusion of FBP
print("e. Few more special methods/functions in Python OOPs (has to be learned)- Instance (important), Class, Static methods")
#There are few more special functions/methods available in Python (Object oriented programming part)
#Instance methods - default/regularly used methods/functions
#Class methods
#Static methods

#7. Scope of Variables in Functions/Methods - Local and Global variable
print("7. Scope of Variables in Functions/Methods - Local and Global variable")
print("a. Local Variable - A variable has scope only within the method/function in a module")
def sal_calc(sal):
    local_bon=1000#This is local variable, because this variable is defined inside the function, only has a scope (accessible) inside the function
    print("bonus variable can be only printed inside the function, because it is local",local_bon)
    return sal+local_bon
print(sal_calc(10000))
#print("bonus variable can be only printed inside the function, because it is local, outside the function it doesn't have the scope",local_bon)

print("b. Global Variable - A variable has scope across the module")
#global global_bon #By default python will write this line of code, we don't need to write this global definition
global_bon = 1000  # This is local variable, because this variable is defined inside the function, only has a scope (accessible) inside the function
def sal_calc(sal):
    print("bonus variable can be printed inside the function or outside the function also, because it is global by default",global_bon)
    return sal+global_bon
print(sal_calc(10000))
print("bonus variable can be printed inside the function or outside the function also, because it is global by default, it has the scope across the module",global_bon)

#Important point to discuss is? How a variable inside the method/function can be made global?
print("c. Local Variable defined as global - A variable has scope only within the method/function in a module by default it is local"
      "Where as, if we define a variable inside the method as global, then it will become global variable and accessible accross the module")
def sal_calc(sal):
    global local_bon
    local_bon=1000#This is local variable, because this variable is defined inside the function, only has a scope (accessible) inside the function
    print("bonus variable can be only printed inside the function, because it is local",local_bon)
    return sal+local_bon
print(sal_calc(10000))
print("bonus variable can be only printed inside the function, because it is local, outside the function it doesn't have the scope",local_bon)